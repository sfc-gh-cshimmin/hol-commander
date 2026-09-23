-- =============================================================================
-- HOL Commander v2 — SiS Deployment Setup SQL
-- Extends the existing GRE_APPS database with infrastructure for the
-- full HOL Commander admin app running as Streamlit-in-Snowflake.
--
-- Prerequisites:
--   - GRE_APPS database already exists
--   - Run as ACCOUNTADMIN
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE SCHEMA GRE_APPS.HOL_COMMANDER_APP;

-- =============================================================================
-- 1. Secrets
-- =============================================================================

-- Shared private key for EMERGENCY_SERVICE_USER (used by all HOL accounts)
CREATE SECRET IF NOT EXISTS GRE_APPS.HOL_COMMANDER_APP.HOL_PRIVATE_KEY
    TYPE = GENERIC_STRING  SECRET_STRING = 'PLACEHOLDER';

-- DataOps.live API token (GitLab PAT)
CREATE SECRET IF NOT EXISTS GRE_APPS.HOL_COMMANDER_APP.DATAOPS_API_TOKEN
    TYPE = GENERIC_STRING  SECRET_STRING = 'PLACEHOLDER';

-- App-level secrets (passwords used in SQL statements and app gate)
CREATE SECRET IF NOT EXISTS GRE_APPS.HOL_COMMANDER_APP.HOL_PASSWORD
    TYPE = GENERIC_STRING  SECRET_STRING = 'PLACEHOLDER';
CREATE SECRET IF NOT EXISTS GRE_APPS.HOL_COMMANDER_APP.HOL_TEMP_PASSWORD
    TYPE = GENERIC_STRING  SECRET_STRING = 'PLACEHOLDER';

-- =============================================================================
-- 2. Additional Tables
-- =============================================================================

-- Favorites — replaces the local JSON file for pinned events
CREATE TABLE IF NOT EXISTS GRE_APPS.HOL_COMMANDER_APP.FAVORITES (
    SLUG        VARCHAR NOT NULL,
    NAME        VARCHAR NOT NULL,
    CREATED_AT  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT uq_favorite_slug UNIQUE (SLUG)
);

-- =============================================================================
-- 3. Network Rule — add DataOps API host alongside existing wildcard
-- =============================================================================
CREATE OR REPLACE NETWORK RULE GRE_APPS.HOL_COMMANDER_APP.SNOWFLAKE_EGRESS_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = (
        '*.snowflakecomputing.com:443',
        'admin.dataops.live:443'
    );

-- =============================================================================
-- 4. External Access Integration
-- =============================================================================
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION HOL_COMMANDER_EAI
    ALLOWED_NETWORK_RULES          = (GRE_APPS.HOL_COMMANDER_APP.SNOWFLAKE_EGRESS_RULE)
    ALLOWED_AUTHENTICATION_SECRETS = (
        GRE_APPS.HOL_COMMANDER_APP.HOL_PRIVATE_KEY,
        GRE_APPS.HOL_COMMANDER_APP.DATAOPS_API_TOKEN,
        GRE_APPS.HOL_COMMANDER_APP.HOL_PASSWORD,        GRE_APPS.HOL_COMMANDER_APP.HOL_TEMP_PASSWORD
    )
    ENABLED = TRUE;

-- =============================================================================
-- 5. Stage (reuse existing or create)
-- =============================================================================
CREATE STAGE IF NOT EXISTS GRE_APPS.HOL_COMMANDER_APP.STREAMLIT_STAGE
    DIRECTORY = (ENABLE = TRUE);

-- =============================================================================
-- 6. Warehouse (reuse existing or create)
-- =============================================================================
CREATE WAREHOUSE IF NOT EXISTS HOL_COMMANDER_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND   = 60
    AUTO_RESUME    = TRUE;

-- =============================================================================
-- 7. Stored Procedure: EXECUTE_REMOTE_SQL_BATCH
--    Connects to an external Snowflake account via JWT key-pair auth and the
--    REST API, then executes an array of SQL statements on a single session.
--
--    Supports three statement types:
--      - "execute": run SQL, return success/error
--      - "query":   run SQL, return columns + rows
--      - "dynamic": run SQL, iterate rows, execute a template for each row
-- =============================================================================
CREATE OR REPLACE PROCEDURE GRE_APPS.HOL_COMMANDER_APP.EXECUTE_REMOTE_SQL_BATCH(
    ACCOUNT_ID VARCHAR,
    USER_NAME VARCHAR,
    ROLE_NAME VARCHAR,
    WAREHOUSE_NAME VARCHAR,
    KEY_SECRET_NAME VARCHAR,
    PASS_SECRET_NAME VARCHAR,
    STATEMENTS_JSON VARCHAR
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'cryptography', 'pyjwt', 'requests')
HANDLER = 'run'
EXTERNAL_ACCESS_INTEGRATIONS = (HOL_COMMANDER_EAI)
SECRETS = (
    'HOL_PRIVATE_KEY'     = GRE_APPS.HOL_COMMANDER_APP.HOL_PRIVATE_KEY,
    'HOL_PASSWORD'        = GRE_APPS.HOL_COMMANDER_APP.HOL_PASSWORD,
    'HOL_TEMP_PASSWORD'   = GRE_APPS.HOL_COMMANDER_APP.HOL_TEMP_PASSWORD
)
AS
$$
import _snowflake
import requests
import jwt
import json
import time
import hashlib
import base64
import uuid
from cryptography.hazmat.primitives import serialization


def _make_jwt(account, user, pem_bytes, passphrase):
    pk = serialization.load_pem_private_key(pem_bytes, password=passphrase)
    pub_der = pk.public_key().public_bytes(
        serialization.Encoding.DER,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    fp = 'SHA256:' + base64.b64encode(hashlib.sha256(pub_der).digest()).decode()
    acct_upper = account.upper()
    user_upper = user.upper()
    qualified = f'{acct_upper}.{user_upper}'
    now = int(time.time())
    payload = {
        'iss': f'{qualified}.{fp}',
        'sub': qualified,
        'iat': now,
        'exp': now + 300,
    }
    return jwt.encode(payload, pk, algorithm='RS256')


def _login(host, account_id, user_name, role_name, warehouse_name, token):
    login_url = f'https://{host}/session/v1/login-request'
    login_body = {
        'data': {
            'ACCOUNT_NAME': account_id.upper(),
            'LOGIN_NAME': user_name,
            'AUTHENTICATOR': 'SNOWFLAKE_JWT',
            'TOKEN': token,
        }
    }
    if role_name and role_name != 'None':
        login_body['data']['ROLE_NAME'] = role_name
    if warehouse_name and warehouse_name != 'None':
        login_body['data']['WAREHOUSE_NAME'] = warehouse_name

    headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
    lr = requests.post(login_url, json=login_body, headers=headers, timeout=30)
    ld = lr.json()
    if not ld.get('success'):
        return None, ld
    return ld['data']['token'], None


def _execute_query(host, session_token, sql_text, timeout=60):
    query_url = f'https://{host}/queries/v1/query-request?requestId={uuid.uuid4()}'
    q_headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f'Snowflake Token="{session_token}"',
    }
    q_body = {
        'sqlText': sql_text,
        'sequenceId': 1,
        'parameters': {'QUERY_RESULT_FORMAT': 'JSON'},
    }
    qr = requests.post(query_url, json=q_body, headers=q_headers, timeout=timeout)
    qd = qr.json()
    if not qd.get('success'):
        msg = qd.get('message', '')
        data = qd.get('data', {})
        if not msg and isinstance(data, dict):
            msg = data.get('sqlState', '') + ': ' + data.get('errorMessage', str(data))
        return {
            'success': False,
            'error': msg or json.dumps(qd),
            'columns': [],
            'rows': [],
        }
    row_type = qd.get('data', {}).get('rowtype', [])
    columns = [c['name'] for c in row_type]
    rows = qd.get('data', {}).get('rowset', [])
    return {'success': True, 'columns': columns, 'rows': rows, 'error': None}


def _resolve_secret(name):
    if not name or name == 'None':
        return None
    try:
        return _snowflake.get_generic_secret_string(name)
    except Exception:
        return None


def run(session, account_id, user_name, role_name, warehouse_name,
        key_secret_name, pass_secret_name, statements_json):
    # Parse statements
    try:
        statements = json.loads(statements_json) if isinstance(statements_json, str) else statements_json
    except (json.JSONDecodeError, TypeError) as e:
        return json.dumps({'login_success': False, 'error': f'Invalid statements JSON: {e}', 'results': []})

    # Load key material
    pem_str = _resolve_secret(key_secret_name)
    if not pem_str or pem_str == 'PLACEHOLDER':
        return json.dumps({'login_success': False, 'error': f'Secret {key_secret_name} not populated', 'results': []})

    passphrase = None
    ps = _resolve_secret(pass_secret_name)
    if ps and ps != 'PLACEHOLDER':
        passphrase = ps.encode('utf-8')

    # Resolve password secrets if needed in SQL templates
    hol_password = _resolve_secret('HOL_PASSWORD') or ''
    hol_temp_password = _resolve_secret('HOL_TEMP_PASSWORD') or ''

    # Generate JWT and login
    try:
        token = _make_jwt(account_id, user_name, pem_str.encode('utf-8'), passphrase)
    except Exception as e:
        return json.dumps({'login_success': False, 'error': f'JWT generation failed: {e}', 'results': []})

    host = f'{account_id}.snowflakecomputing.com'
    session_token, login_err = _login(host, account_id, user_name, role_name, warehouse_name, token)
    if not session_token:
        return json.dumps({'login_success': False, 'error': f'Login failed: {json.dumps(login_err)}', 'results': []})

    # Execute statements
    results = []
    for idx, stmt_obj in enumerate(statements):
        if isinstance(stmt_obj, str):
            stmt_obj = {'sql': stmt_obj, 'type': 'execute'}

        sql = stmt_obj.get('sql', '')
        stmt_type = stmt_obj.get('type', 'execute')
        service_key = stmt_obj.get('service_key', '')

        # Substitute password placeholders
        sql = sql.replace('{HOL_PASSWORD}', hol_password)
        sql = sql.replace('{HOL_TEMP_PASSWORD}', hol_temp_password)

        if stmt_type == 'dynamic':
            # Run the SQL, then for each row execute the template
            show_result = _execute_query(host, session_token, sql)
            sub_results = []
            if show_result['success'] and show_result['rows']:
                template = stmt_obj.get('dynamic_template', '')
                # Find the column index for "name" (case-insensitive)
                cols_lower = [c.lower() for c in show_result['columns']]
                name_idx = cols_lower.index('name') if 'name' in cols_lower else 0
                for row in show_result['rows']:
                    val = row[name_idx] if name_idx < len(row) else row[0]
                    alter_sql = template.replace('{name}', str(val))
                    alter_result = _execute_query(host, session_token, alter_sql)
                    sub_results.append({
                        'sql': alter_sql,
                        'success': alter_result['success'],
                        'error': alter_result.get('error'),
                    })
            results.append({
                'index': idx,
                'success': show_result['success'] and all(s['success'] for s in sub_results),
                'error': show_result.get('error'),
                'columns': show_result.get('columns', []),
                'rows': show_result.get('rows', []),
                'sub_results': sub_results,
                'service_key': service_key,
                'type': 'dynamic',
            })
        else:
            qr = _execute_query(host, session_token, sql)
            results.append({
                'index': idx,
                'success': qr['success'],
                'error': qr.get('error'),
                'columns': qr.get('columns', []),
                'rows': qr.get('rows', []),
                'service_key': service_key,
                'type': stmt_type,
            })

    return json.dumps({'login_success': True, 'results': results})
$$;

-- =============================================================================
-- 8. Stored Procedure: EXECUTE_DATAOPS_API
--    Proxies HTTP GET/POST requests to admin.dataops.live through the EAI.
-- =============================================================================
CREATE OR REPLACE PROCEDURE GRE_APPS.HOL_COMMANDER_APP.EXECUTE_DATAOPS_API(
    METHOD VARCHAR,
    PATH VARCHAR,
    PARAMS_JSON VARCHAR,
    BODY_JSON VARCHAR,
    AUTH_METHOD_HINT VARCHAR
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests')
HANDLER = 'run'
EXTERNAL_ACCESS_INTEGRATIONS = (HOL_COMMANDER_EAI)
SECRETS = (
    'DATAOPS_API_TOKEN' = GRE_APPS.HOL_COMMANDER_APP.DATAOPS_API_TOKEN
)
AS
$$
import _snowflake
import requests
import json

BASE_URL = 'https://admin.dataops.live/api/v1'


def _headers(token, method):
    if method == 'pat':
        return {'private-token': token, 'Content-Type': 'application/json', 'Accept': 'application/json'}
    return {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json', 'Accept': 'application/json'}


def run(session, method, path, params_json, body_json, auth_method_hint):
    token = _snowflake.get_generic_secret_string('DATAOPS_API_TOKEN')
    if not token or token == 'PLACEHOLDER':
        return json.dumps({'error': 'DATAOPS_API_TOKEN secret not populated', 'status_code': 0})

    params = json.loads(params_json) if params_json else None
    body = json.loads(body_json) if body_json else None

    url = f'{BASE_URL}{path}'
    methods_to_try = []
    if auth_method_hint and auth_method_hint != 'None':
        methods_to_try.append(auth_method_hint)
        other = 'bearer' if auth_method_hint == 'pat' else 'pat'
        methods_to_try.append(other)
    else:
        methods_to_try = ['pat', 'bearer']

    last_resp = None
    for auth_method in methods_to_try:
        hdrs = _headers(token, auth_method)
        try:
            if method.upper() == 'POST':
                resp = requests.post(url, headers=hdrs, params=params, json=body, timeout=30)
            else:
                resp = requests.get(url, headers=hdrs, params=params, timeout=30)

            if resp.status_code not in (401, 403):
                try:
                    resp_body = resp.json()
                except Exception:
                    resp_body = resp.text
                return json.dumps({
                    'status_code': resp.status_code,
                    'body': resp_body,
                    'auth_method': auth_method,
                    'error': None if resp.status_code < 400 else f'HTTP {resp.status_code}',
                })
            last_resp = resp
        except requests.exceptions.RequestException as e:
            return json.dumps({'error': str(e), 'status_code': 0, 'auth_method': auth_method})

    # All auth methods returned 401/403
    return json.dumps({
        'error': f'Authentication failed (tried {", ".join(methods_to_try)})',
        'status_code': last_resp.status_code if last_resp else 0,
        'auth_method': None,
    })
$$;

-- =============================================================================
-- 9. Upload app files to stage
--    Run these from SnowSQL or a worksheet after saving the files locally:
--
--    PUT file:///path/to/streamlit_app.py @GRE_APPS.HOL_COMMANDER_APP.STREAMLIT_STAGE/hol_commander_v2 OVERWRITE=TRUE AUTO_COMPRESS=FALSE;
--    PUT file:///path/to/environment.yml  @GRE_APPS.HOL_COMMANDER_APP.STREAMLIT_STAGE/hol_commander_v2 OVERWRITE=TRUE AUTO_COMPRESS=FALSE;
-- =============================================================================

-- =============================================================================
-- 10. Create the Streamlit app
--     All secrets the app or its called procedures need must be listed here.
-- =============================================================================
CREATE OR REPLACE STREAMLIT GRE_APPS.HOL_COMMANDER_APP.HOL_COMMANDER_V2
    ROOT_LOCATION                  = '@GRE_APPS.HOL_COMMANDER_APP.STREAMLIT_STAGE/hol_commander_v2'
    MAIN_FILE                      = 'streamlit_app.py'
    QUERY_WAREHOUSE                = HOL_COMMANDER_WH
    EXTERNAL_ACCESS_INTEGRATIONS   = (HOL_COMMANDER_EAI)
    SECRETS                        = (
        'HOL_PRIVATE_KEY'     = GRE_APPS.HOL_COMMANDER_APP.HOL_PRIVATE_KEY,
        'DATAOPS_API_TOKEN'   = GRE_APPS.HOL_COMMANDER_APP.DATAOPS_API_TOKEN,
        'HOL_PASSWORD'        = GRE_APPS.HOL_COMMANDER_APP.HOL_PASSWORD,
        'HOL_TEMP_PASSWORD'   = GRE_APPS.HOL_COMMANDER_APP.HOL_TEMP_PASSWORD
    );

-- =============================================================================
-- 11. Grant access
-- =============================================================================
-- GRANT USAGE ON DATABASE GRE_APPS              TO ROLE <role_name>;
-- GRANT USAGE ON SCHEMA GRE_APPS.HOL_COMMANDER_APP      TO ROLE <role_name>;
-- GRANT USAGE ON WAREHOUSE HOL_COMMANDER_WH    TO ROLE <role_name>;
-- GRANT USAGE ON STREAMLIT GRE_APPS.HOL_COMMANDER_APP.HOL_COMMANDER_V2 TO ROLE <role_name>;

-- =============================================================================
-- 12. Populate secrets with real values
--
--  ALTER SECRET GRE_APPS.HOL_COMMANDER_APP.HOL_PRIVATE_KEY
--      SET SECRET_STRING = '-----BEGIN PRIVATE KEY-----
--  MIIEv...
--  -----END PRIVATE KEY-----';
--
--  ALTER SECRET GRE_APPS.HOL_COMMANDER_APP.DATAOPS_API_TOKEN
--      SET SECRET_STRING = 'glpat-xxxxxxxxxxxxxxxxxxxx';
--
--  ALTER SECRET GRE_APPS.HOL_COMMANDER_APP.HOL_PASSWORD
--      SET SECRET_STRING = 'the_hol_password';
--
--  ALTER SECRET GRE_APPS.HOL_COMMANDER_APP.HOL_TEMP_PASSWORD
--      SET SECRET_STRING = 'the_temp_password';
--
-- =============================================================================
