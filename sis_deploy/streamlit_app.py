"""
HOL Commander v2 — Streamlit-in-Snowflake Edition
Refactored from app.py to run inside the Snowflake sandbox.
All external connectivity is proxied through stored procedures.
"""

import re
import json
import time
import threading
import _snowflake
import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional


# =============================================================================
# Compatibility helper
# =============================================================================

def _rerun():
    if hasattr(st, 'rerun'):
        st.rerun()
    else:
        st.experimental_rerun()

# =============================================================================
# Page configuration — must be the first Streamlit call
# =============================================================================

st.set_page_config(
    page_title="DataOps.live HOL Commander",
    layout="wide"
)

# =============================================================================
# Snowpark session
# =============================================================================

@st.cache_resource
def _get_session():
    return get_active_session()


def _run_local_sql(sql, params=None):
    session = _get_session()
    if params:
        return session.sql(sql, params=params).collect()
    return session.sql(sql).collect()


# =============================================================================
# Secret helpers
# =============================================================================

def _get_secret(name: str) -> str:
    try:
        val = _snowflake.get_generic_secret_string(name)
        return val if val and val != 'PLACEHOLDER' else ''
    except Exception:
        return ''


# =============================================================================
# Configuration
# =============================================================================

HARDCODED_EVENTS = [
    {"slug": "launchpad-industry-demos", "name": "Launchpad Industry Demos"},
]

DEFAULT_SERVICE_USER = "EMERGENCY_SERVICE_USER"
DEFAULT_KEY_SECRET = "HOL_PRIVATE_KEY"
DEFAULT_PASS_SECRET = None  # key is unencrypted, no passphrase needed

ACCOUNTS_TABLE = "GRE_APPS.HOL_COMMANDER_APP.ACCOUNTS"
FAVORITES_TABLE = "GRE_APPS.HOL_COMMANDER_APP.FAVORITES"


# =============================================================================
# Module-level apply job state
# =============================================================================

@st.cache_resource
def _make_apply_state():
    return threading.Lock(), {
        "running": False,
        "cancel": threading.Event(),
        "results": [],
        "circles": [],
        "completed": 0,
        "total": 0,
        "status": "",
        "cancelled": False,
    }

_apply_lock, _apply_job = _make_apply_state()

# =============================================================================
# DataOps API Client — proxied through EXECUTE_DATAOPS_API stored procedure
# =============================================================================

class DataOpsClient:
    def __init__(self):
        self.auth_method: Optional[str] = None

    def _call_api(self, method: str, path: str, params: Optional[dict] = None, body: Optional[dict] = None):
        session = _get_session()
        result = session.sql(
            "CALL GRE_APPS.HOL_COMMANDER_APP.EXECUTE_DATAOPS_API(?, ?, ?, ?, ?)",
            params=[
                method,
                path,
                json.dumps(params) if params else None,
                json.dumps(body) if body else None,
                self.auth_method,
            ],
        ).collect()
        raw = result[0][0]
        data = json.loads(raw) if isinstance(raw, str) else raw
        if isinstance(data, str):
            data = json.loads(data)

        if data.get("auth_method"):
            self.auth_method = data["auth_method"]
        if data.get("error"):
            raise RuntimeError(data["error"])
        return data.get("body")

    def _get(self, path: str, params: Optional[dict] = None):
        return self._call_api("GET", path, params)

    def _post(self, path: str, json_data: Optional[dict] = None, params: Optional[dict] = None):
        return self._call_api("POST", path, params, json_data)

    def rerun_configure_pipeline(self, slug: str):
        return self._post(f"/event_management/{slug}/rerun_configure_pipeline")

    def health_check(self):
        return self._get("/health_check")

    def get_events(self, search: Optional[str] = None):
        params = {}
        if search:
            params["search"] = search
        return self._get("/event_management/events-paginated", params or None)

    def get_all_events(self):
        return self._get("/event_management/events")

    def get_event(self, slug: str):
        return self._get(f"/event_management/events/{slug}")

    def get_event_details(self, slug: str):
        return self._get(f"/event_management/events/{slug}/details")

    def get_event_accounts(self, slug: str, page: int = 1, page_size: int = 100, search: Optional[str] = None):
        params = {"page": page, "page_size": page_size}
        if search:
            params["search"] = search
        return self._get(f"/event_management/events/{slug}/accounts", params)

    def decommission_account(self, event_slug: str, account_id: int, remain_allocated: bool = True) -> dict:
        return self._post(
            f"/event_management/events/{event_slug}/accounts/{account_id}/decommission",
            params={"remain_allocated": str(remain_allocated).lower()},
        )

    def get_all_event_accounts(self, slug: str) -> List[dict]:
        all_accounts = []
        page = 1
        while True:
            resp = self.get_event_accounts(slug, page=page, page_size=100)
            if isinstance(resp, dict):
                items = resp.get("items", resp.get("accounts", resp.get("results", [])))
                total = resp.get("total", resp.get("total_count", None))
            elif isinstance(resp, list):
                items = resp
                total = None
            else:
                break

            if not items:
                break
            all_accounts.extend(items)

            if total is not None and len(all_accounts) >= total:
                break
            if len(items) < 100:
                break
            page += 1
        return all_accounts


# =============================================================================
# Favorites helpers — backed by FAVORITES table
# =============================================================================

def load_favorites() -> List[Dict]:
    rows = _run_local_sql(f"SELECT SLUG, NAME FROM {FAVORITES_TABLE} ORDER BY CREATED_AT")
    return [{"slug": r["SLUG"], "name": r["NAME"]} for r in rows] if rows else []


def save_favorite(slug: str, name: str):
    _run_local_sql(
        f"INSERT INTO {FAVORITES_TABLE} (SLUG, NAME) SELECT ?, ? WHERE NOT EXISTS (SELECT 1 FROM {FAVORITES_TABLE} WHERE SLUG = ?)",
        params=[slug, name, slug],
    )


def remove_favorite(slug: str):
    _run_local_sql(f"DELETE FROM {FAVORITES_TABLE} WHERE SLUG = ?", params=[slug])


def get_pinned_events() -> List[Dict]:
    favorites = load_favorites()
    seen_slugs = {e["slug"] for e in HARDCODED_EVENTS}
    combined = list(HARDCODED_EVENTS)
    for fav in favorites:
        if fav.get("slug") not in seen_slugs:
            combined.append(fav)
            seen_slugs.add(fav["slug"])
    return combined


def add_favorite(slug: str, name: str):
    save_favorite(slug, name)


# =============================================================================
# Account mapping
# =============================================================================

def _locator_from_url(url: str) -> str:
    if url:
        m = re.match(r'https?://([^.]+)\.snowflakecomputing\.com', url, re.IGNORECASE)
        if m:
            return m.group(1).lower()
    return ""


def api_account_to_internal(api_acc: dict) -> dict:
    identifier = api_acc.get("identifier", "")
    slug = api_acc.get("slug", "")
    account_id = identifier or slug
    if isinstance(account_id, int):
        account_id = str(account_id)
    email = api_acc.get("allocated_to") or ""
    status = api_acc.get("status", "")
    url = api_acc.get("url", "")
    conn_account = _locator_from_url(url) or (f"sfsehol-{identifier}".lower().replace("_", "-") if identifier else "")
    suffix = identifier.split("_")[-1] if "_" in identifier else slug
    return {
        "account_id": identifier or slug,
        "api_id": api_acc.get("id"),
        "suffix": suffix,
        "status": status,
        "assigned_to": email,
        "url": url,
        "conn_account": conn_account,
        "_raw": api_acc,
    }


# =============================================================================
# Session state initialization
# =============================================================================

st.session_state.setdefault("selected_accounts", set())
st.session_state.setdefault("results", [])
st.session_state.setdefault("mfa_bypass_minutes", 60)
st.session_state.setdefault("password_reset_must_change", True)
st.session_state.setdefault("password_reset_disable_mfa", True)
st.session_state.setdefault("target_user", "USER")
st.session_state.setdefault("parallel_execution", False)
st.session_state.setdefault("parallel_workers", 5)
st.session_state.setdefault("disable_mfa_comment", "Disabled for SI event")
st.session_state.setdefault("mfa_bypass_task_define", False)
st.session_state.setdefault("search_clear_count", 0)
st.session_state.setdefault("selected_event_slug", None)
st.session_state.setdefault("api_accounts", [])
st.session_state.setdefault("api_accounts_raw", [])
st.session_state.setdefault("event_search_results", [])
st.session_state.setdefault("dataops_connected", False)
st.session_state.setdefault("dataops_auth_method", None)
st.session_state.setdefault("active_services", set())
st.session_state.setdefault("decommission_remain_allocated", True)
st.session_state.setdefault("account_source_events", {})


# =============================================================================
# Helper functions
# =============================================================================

def fuzzy_match(query: str, email: str) -> bool:
    if not query.strip():
        return True
    normalise = lambda s: re.sub(r'[.@_\-]', ' ', s).lower()
    norm_email = normalise(email)
    tokens = normalise(query).split()
    return all(tok in norm_email for tok in tokens)


def parse_account_csv(csv_text: str) -> List[Dict]:
    if not csv_text or not csv_text.strip():
        return []
    accounts = []
    lines = csv_text.strip().split('\n')
    start_idx = 0
    if lines and 'account' in lines[0].lower():
        start_idx = 1
    for line in lines[start_idx:]:
        line = line.strip()
        if not line:
            continue
        parts = line.split(',')
        if len(parts) < 1:
            continue
        account_id = parts[0].strip()
        status = parts[1].strip() if len(parts) > 1 else ""
        assigned_to = parts[2].strip() if len(parts) > 2 else ""
        url = parts[3].strip() if len(parts) > 3 else ""
        suffix = account_id.split('_')[-1] if '_' in account_id else account_id[-6:]
        conn_account = _locator_from_url(url) or f"sfsehol-{account_id}".replace("_", "-")
        accounts.append({
            "account_id": account_id,
            "suffix": suffix,
            "status": status,
            "assigned_to": assigned_to,
            "url": url,
            "conn_account": conn_account.lower()
        })
    return accounts


def resolve_target_user() -> str:
    target = st.session_state.get("target_user", "USER")
    if target == "Custom":
        return st.session_state.get("custom_target_user", "").strip() or "USER"
    return target


# =============================================================================
# Service SQL generators (same as app.py but with password placeholders)
# =============================================================================

def _hol_password() -> str:
    return '{HOL_PASSWORD}'

def _hol_temp_password() -> str:
    return '{HOL_TEMP_PASSWORD}'


def get_password_reset_statements() -> List[str]:
    target = resolve_target_user()
    stmts = []
    if st.session_state.get("password_reset_disable_mfa", True):
        stmts.append(f"ALTER USER {target} SET MINS_TO_BYPASS_MFA = 60")
    stmts += [
        "USE DATABASE POLICY_DB",
        "USE SCHEMA POLICIES",
        "ALTER ACCOUNT UNSET PASSWORD POLICY",
        f"ALTER USER {target} UNSET PASSWORD POLICY",
        f"""CREATE OR REPLACE PASSWORD POLICY my_policy
  PASSWORD_MIN_LENGTH = 8
  PASSWORD_MIN_UPPER_CASE_CHARS = 0
  PASSWORD_MIN_LOWER_CASE_CHARS = 0
  PASSWORD_MIN_NUMERIC_CHARS = 0
  PASSWORD_MIN_SPECIAL_CHARS = 0
  PASSWORD_HISTORY = 0""",
        "ALTER ACCOUNT SET PASSWORD POLICY my_policy",
        f"ALTER USER {target} SET PASSWORD = '{_hol_temp_password()}'",
        f"ALTER USER {target} SET PASSWORD = '{_hol_password()}'",
    ]
    if st.session_state.get("password_reset_must_change", True):
        stmts.append(f"ALTER USER {target} SET MUST_CHANGE_PASSWORD = TRUE")
    return stmts


def render_password_reset_config():
    st.checkbox(
        "Disable MFA before resetting password",
        key="password_reset_disable_mfa",
        help="Runs ALTER USER USER SET MINS_TO_BYPASS_MFA = 60 before the password reset steps",
    )
    st.checkbox(
        "Require password change on next login",
        key="password_reset_must_change",
        help="Sets MUST_CHANGE_PASSWORD = TRUE on the USER after resetting",
    )


def get_password_reset_preview() -> str:
    stmts = get_password_reset_statements()
    return "\n\n".join(s + ";" for s in stmts)


def get_mfa_bypass_statements() -> List[str]:
    target = resolve_target_user()
    minutes = st.session_state.get("mfa_bypass_minutes", 60)
    return [f"ALTER USER {target} SET MINS_TO_BYPASS_MFA = {int(minutes)}"]


def get_mfa_bypass_preview() -> str:
    return "\n\n".join(s + ";" for s in get_mfa_bypass_statements())


_MFA_BYPASS_TASK_DDL = """CREATE OR REPLACE TASK POLICY_DB.POLICIES.MFA_USERS_BYPASS
    SCHEDULE = '23 HOURS'
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AS
EXECUTE IMMEDIATE $$
DECLARE
    user_cursor RESULTSET;
    user_name STRING;
    show_qid STRING;
BEGIN
    SHOW USERS;
    show_qid := LAST_QUERY_ID();
    user_cursor := (SELECT "name" AS USERNAME FROM TABLE(RESULT_SCAN(:show_qid)) WHERE "disabled" = FALSE AND "type" = 'PERSON');
    FOR user_record IN user_cursor DO
        user_name := user_record.USERNAME;
        ALTER USER identifier(:user_name) SET MINS_TO_BYPASS_MFA = 1440;
    END FOR;
END;
$$"""

_MFA_BYPASS_TASK_RESUME = "ALTER TASK POLICY_DB.POLICIES.MFA_USERS_BYPASS RESUME"


def render_mfa_bypass_task_config():
    st.checkbox(
        "Define (create/replace) task first",
        key="mfa_bypass_task_define",
        help="Runs CREATE OR REPLACE TASK + ALTER TASK RESUME before the selected action.",
    )
    if st.session_state.get("mfa_bypass_task_action") not in ("execute", "suspend"):
        st.session_state["mfa_bypass_task_action"] = "suspend"
    st.radio(
        "Task action",
        options=["execute", "suspend"],
        key="mfa_bypass_task_action",
        horizontal=True,
        help="Execute runs the task immediately. Suspend stops the task from running on its schedule.",
    )


def get_mfa_bypass_task_statements():
    stmts = []
    if st.session_state.get("mfa_bypass_task_define", False):
        stmts += [_MFA_BYPASS_TASK_DDL, _MFA_BYPASS_TASK_RESUME]
    if st.session_state.get("mfa_bypass_task_action") == "execute":
        stmts.append("EXECUTE TASK POLICY_DB.POLICIES.MFA_USERS_BYPASS")
    else:
        stmts.append("ALTER TASK POLICY_DB.POLICIES.MFA_USERS_BYPASS SUSPEND")
    return stmts


def get_mfa_bypass_task_preview():
    stmts = get_mfa_bypass_task_statements()
    return "\n\n".join(s + ";" for s in stmts)


def render_mfa_config():
    st.number_input(
        "Minutes to disable MFA",
        min_value=0,
        max_value=10080,
        value=st.session_state["mfa_bypass_minutes"],
        step=15,
        key="mfa_bypass_minutes",
        help="How long MFA will be bypassed for the USER user (0-10080 minutes). Set to 0 to disable the bypass.",
    )


def get_remove_mfa_method_config() -> dict:
    target = resolve_target_user()
    return {
        "show_sql": f"SHOW MFA METHODS FOR USER {target}",
        "alter_template": f"ALTER USER {target} REMOVE MFA METHOD {{name}}",
    }


def get_remove_mfa_method_preview() -> str:
    target = resolve_target_user()
    return (
        f"SHOW MFA METHODS FOR USER {target};\n\n"
        f"-- Then, for each row in the result:\n"
        f"ALTER USER {target} REMOVE MFA METHOD <name>;"
    )


def get_coco_cross_region_statements() -> List[str]:
    stmts = [
        "USE ROLE ACCOUNTADMIN",
        "ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION'",
        'GRANT APPLICATION ROLE SNOWFLAKE."CORTEX-MODEL-ROLE-ALL" TO ROLE PUBLIC',
        "GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE ATTENDEE_ROLE",
        "GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE PUBLIC",
    ]
    if st.session_state.get("coco_cross_region_refresh_models", False):
        stmts.append("CALL SNOWFLAKE.MODELS.CORTEX_BASE_MODELS_REFRESH()")
    return stmts


def render_coco_cross_region_config():
    st.checkbox(
        "Refresh Cortex base models",
        key="coco_cross_region_refresh_models",
        help="Also calls SNOWFLAKE.MODELS.CORTEX_BASE_MODELS_REFRESH() after applying the account settings.",
    )


def get_coco_cross_region_preview() -> str:
    return "\n\n".join(s + ";" for s in get_coco_cross_region_statements())


def get_patch_auth_policy_statements() -> List[str]:
    return [
        "USE ROLE ACCOUNTADMIN",
        "ALTER ACCOUNT UNSET AUTHENTICATION POLICY",
        """ALTER AUTHENTICATION POLICY POLICY_DB.POLICIES.event_authentication_policy SET
  MFA_ENROLLMENT=REQUIRED
  CLIENT_TYPES = ('ALL')
  AUTHENTICATION_METHODS = ('ALL')""",
        "ALTER ACCOUNT SET AUTHENTICATION POLICY POLICY_DB.POLICIES.event_authentication_policy",
    ]


def get_patch_auth_policy_preview() -> str:
    return "\n\n".join(s + ";" for s in get_patch_auth_policy_statements())


def parse_sql_statements(sql_text: str) -> List[str]:
    sql_text = re.sub(r'```\w*\n?', '', sql_text).strip()
    statements: List[str] = []
    current: List[str] = []
    i, n = 0, len(sql_text)
    in_dollar_quote = False
    in_single_quote = False
    in_line_comment = False
    in_block_comment = False
    while i < n:
        ch = sql_text[i]
        if in_line_comment:
            current.append(ch)
            if ch == '\n':
                in_line_comment = False
            i += 1
        elif in_block_comment:
            if sql_text[i:i+2] == '*/':
                current.append('*/')
                in_block_comment = False
                i += 2
            else:
                current.append(ch)
                i += 1
        elif in_dollar_quote:
            if sql_text[i:i+2] == '$$':
                current.append('$$')
                in_dollar_quote = False
                i += 2
            else:
                current.append(ch)
                i += 1
        elif in_single_quote:
            current.append(ch)
            if ch == "'":
                if i + 1 < n and sql_text[i+1] == "'":
                    current.append("'")
                    i += 2
                else:
                    in_single_quote = False
                    i += 1
            else:
                i += 1
        else:
            if sql_text[i:i+2] == '--':
                in_line_comment = True
                current.append('-')
                i += 1
            elif sql_text[i:i+2] == '/*':
                in_block_comment = True
                current.append('/*')
                i += 2
            elif sql_text[i:i+2] == '$$':
                in_dollar_quote = True
                current.append('$$')
                i += 2
            elif ch == "'":
                in_single_quote = True
                current.append(ch)
                i += 1
            elif ch == ';':
                stmt = ''.join(current).strip()
                lines = [l.strip() for l in stmt.split('\n')
                         if l.strip() and not l.strip().startswith('--')]
                if lines:
                    statements.append(stmt)
                current = []
                i += 1
            else:
                current.append(ch)
                i += 1
    remaining = ''.join(current).strip()
    lines = [l.strip() for l in remaining.split('\n')
             if l.strip() and not l.strip().startswith('--')]
    if lines:
        statements.append(remaining)
    return statements


def get_custom_sql_statements() -> List[str]:
    all_stmts = []
    num_blocks = st.session_state.get("custom_sql_block_count", 1)
    for i in range(num_blocks):
        if not st.session_state.get(f"custom_sql_block_enabled_{i}", True):
            continue
        sql_text = st.session_state.get(f"custom_sql_input_{i}", "")
        all_stmts.extend(parse_sql_statements(sql_text))
    return all_stmts


def get_custom_sql_preview() -> str:
    stmts = get_custom_sql_statements()
    if not stmts:
        return "-- No SQL statements entered"
    return "\n\n".join(s + ";" for s in stmts)


def render_custom_sql_config():
    st.session_state.setdefault("custom_sql_block_count", 1)
    num_blocks = st.session_state["custom_sql_block_count"]
    for i in range(num_blocks):
        enabled_key = f"custom_sql_block_enabled_{i}"
        st.session_state.setdefault(enabled_key, True)
        enabled = st.checkbox(
            f"SQL Block {i + 1}" if num_blocks > 1 else "SQL Statements",
            key=enabled_key,
            help="Uncheck to skip this block during execution.",
        )
        st.text_area(
            f"SQL Block {i + 1}" if num_blocks > 1 else "SQL Statements",
            key=f"custom_sql_input_{i}",
            height=150,
            placeholder="ALTER USER USER SET MINS_TO_BYPASS_MFA = 60;\nSHOW WAREHOUSES;\nSELECT CURRENT_ACCOUNT();",
            help="Enter one or more SQL statements separated by semicolons.",
            disabled=not enabled,
            label_visibility="collapsed",
        )
    if st.button("➕ Add SQL block", key="add_sql_block"):
        st.session_state["custom_sql_block_count"] = num_blocks + 1
        _rerun()
    if num_blocks > 1:
        if st.button("➖ Remove last block", key="remove_sql_block"):
            last_key = f"custom_sql_input_{num_blocks - 1}"
            if last_key in st.session_state:
                del st.session_state[last_key]
            enabled_key = f"custom_sql_block_enabled_{num_blocks - 1}"
            if enabled_key in st.session_state:
                del st.session_state[enabled_key]
            st.session_state["custom_sql_block_count"] = num_blocks - 1
            _rerun()
    stmts = get_custom_sql_statements()
    if stmts:
        st.caption(f"📝 **{len(stmts)}** statement(s) will be executed per account")


# =============================================================================
# Decommission service helpers
# =============================================================================

def render_decommission_config():
    st.checkbox(
        "Keep account allocated after decommission",
        key="decommission_remain_allocated",
        help="If checked, the account remains allocated but is decommissioned.",
    )


def get_decommission_preview() -> str:
    remain = st.session_state.get("decommission_remain_allocated", True)
    return f"POST /event_management/events/{{event_slug}}/accounts/{{id}}/decommission?remain_allocated={str(remain).lower()}"


def execute_decommission(client: DataOpsClient, event_slug: str, account: Dict, config: Dict = None) -> Dict:
    remain = config.get("remain_allocated", True) if config else True
    api_id = account.get("api_id") or account.get("_raw", {}).get("id")
    if not api_id:
        raise ValueError(f"No numeric API id found for account {account.get('account_id')}")
    try:
        return client.decommission_account(event_slug, int(api_id), remain_allocated=remain)
    except RuntimeError as e:
        err_str = str(e)
        if "500" in err_str or "HTTP 500" in err_str:
            try:
                acc_resp = client._get(f"/event_management/events/{event_slug}/accounts/{api_id}")
                status = acc_resp.get("status", "").lower() if isinstance(acc_resp, dict) else ""
                if "decommission" in status:
                    return {
                        "message": "Decommission likely succeeded (server returned 500 but account status is now decommissioned)",
                        "account_id": int(api_id),
                        "status": status,
                    }
            except Exception:
                pass
        raise


# =============================================================================
# Service definitions
# =============================================================================

SERVICES = {
    "password_reset": {
        "service_type": "action",
        "label": "Reset HOL password",
        "description": "Resets the target user's password and applies a permissive password policy.",
        "icon": "🔐",
        "get_statements": get_password_reset_statements,
        "get_preview": get_password_reset_preview,
        "render_config": render_password_reset_config,
    },
    "remove_mfa_method": {
        "service_type": "dynamic_action",
        "label": "Remove MFA methods",
        "description": "Removes all registered MFA methods from the target user.",
        "icon": "🔓",
        "get_dynamic_config": get_remove_mfa_method_config,
        "get_preview": get_remove_mfa_method_preview,
    },
    "mfa_bypass_task": {
        "service_type": "action",
        "label": "MFA bypass task",
        "description": "Execute or suspend the MFA_USERS_BYPASS task.",
        "icon": "⏸️",
        "get_statements": get_mfa_bypass_task_statements,
        "get_preview": get_mfa_bypass_task_preview,
        "render_config": render_mfa_bypass_task_config,
    },
    "mfa_disable": {
        "service_type": "action",
        "label": "Disable MFA temporarily",
        "description": "Bypasses MFA for the target user for a configurable number of minutes.",
        "icon": "🔒",
        "get_statements": get_mfa_bypass_statements,
        "get_preview": get_mfa_bypass_preview,
        "render_config": render_mfa_config,
    },
    "account_locator": {
        "service_type": "output",
        "label": "Get account locator",
        "description": "Retrieves the Snowflake account locator for each selected account.",
        "icon": "📌",
        "get_statements": lambda: ["SELECT CURRENT_ACCOUNT() AS ACCOUNT_LOCATOR"],
        "get_preview": lambda: "SELECT CURRENT_ACCOUNT() AS ACCOUNT_LOCATOR",
        "output_column": "ACCOUNT_LOCATOR",
    },
    "patch_auth_policy": {
        "service_type": "action",
        "label": "Patch authentication policy",
        "description": "Unsets existing auth policy, updates event_authentication_policy to require MFA, then re-applies it.",
        "icon": "📜",
        "get_statements": get_patch_auth_policy_statements,
        "get_preview": get_patch_auth_policy_preview,
    },
    "coco_cross_region": {
        "service_type": "action",
        "label": "Enable CoCo cross-region",
        "description": "Sets CORTEX_ENABLED_CROSS_REGION and grants CORTEX_USER role.",
        "icon": "🌐",
        "get_statements": get_coco_cross_region_statements,
        "get_preview": get_coco_cross_region_preview,
        "render_config": render_coco_cross_region_config,
    },
    "custom_sql": {
        "service_type": "custom_sql",
        "label": "Run custom SQL",
        "description": "Execute arbitrary SQL statements on each selected account.",
        "icon": "💻",
        "get_statements": get_custom_sql_statements,
        "get_preview": get_custom_sql_preview,
        "render_config": render_custom_sql_config,
    },
    "decommission": {
        "service_type": "api_action",
        "label": "Decommission account",
        "description": "Decommissions the account via the DataOps API.",
        "icon": "🗑️",
        "render_config": render_decommission_config,
        "get_preview": get_decommission_preview,
        "execute": execute_decommission,
    },
}

SERVICE_GROUPS = {
    "disable_account_mfa": {
        "label": "Disable account MFA",
        "icon": "🛡️",
        "description": "Two-part process to disable MFA enforcement on accounts.",
    },
}


# =============================================================================
# Service config resolution (must run on main thread before spawning workers)
# =============================================================================

def resolve_service_configs(selected_services: List[str]) -> Dict[str, Dict]:
    configs = {}
    for svc_key in selected_services:
        svc = SERVICES[svc_key]
        cfg: Dict = {"service_type": svc.get("service_type", "action")}
        if "get_statements" in svc:
            cfg["statements"] = svc["get_statements"]()
        if cfg["service_type"] == "api_action":
            cfg["execute"] = svc["execute"]
            cfg["remain_allocated"] = st.session_state.get("decommission_remain_allocated", True)
        if cfg["service_type"] == "dynamic_action":
            cfg.update(svc["get_dynamic_config"]())
        if "output_column" in svc:
            cfg["output_column"] = svc["output_column"]
        configs[svc_key] = cfg
    return configs


# =============================================================================
# Execution engine — calls stored procedures instead of direct connections
# =============================================================================

def _run_services_core(
    account: Dict,
    service_configs: Dict[str, Dict],
    api_client: Optional[DataOpsClient] = None,
    event_slug: Optional[str] = None,
) -> Dict:
    result = {
        "account_id": account["account_id"],
        "suffix": account["suffix"],
        "assigned_to": account.get("assigned_to", ""),
        "url": account.get("url", ""),
        "conn_account": account["conn_account"],
        "services": {},
        "success": False,
        "error": None,
    }

    all_ok = True

    # --- API-action services (DataOps API calls, no Snowflake connection) ---
    api_configs = {k: v for k, v in service_configs.items() if v.get("service_type") == "api_action"}
    sql_configs = {k: v for k, v in service_configs.items() if v.get("service_type") != "api_action"}

    for svc_key, cfg in api_configs.items():
        svc_result = {"success": False, "error": None}
        try:
            if not api_client:
                raise ValueError("DataOps API client not available")
            if not event_slug:
                raise ValueError("Event slug not available")
            resp = cfg["execute"](api_client, event_slug, account, cfg)
            svc_result["success"] = True
            svc_result["data"] = resp
        except Exception as e:
            svc_result["error"] = str(e)
            all_ok = False
        result["services"][svc_key] = svc_result

    # --- SQL-based services (proxied through EXECUTE_REMOTE_SQL_BATCH) ---
    if sql_configs:
        # Build the batch statement array
        statements = []
        stmt_svc_map = []  # tracks which service each statement belongs to

        for svc_key, cfg in sql_configs.items():
            if cfg["service_type"] == "dynamic_action":
                statements.append({
                    "sql": cfg["show_sql"],
                    "type": "dynamic",
                    "dynamic_template": cfg["alter_template"],
                    "service_key": svc_key,
                })
                stmt_svc_map.append(svc_key)
            else:
                svc_stmts = cfg.get("statements", [])
                stmt_type = "query" if cfg.get("output_column") else "execute"
                for stmt in svc_stmts:
                    statements.append({
                        "sql": stmt,
                        "type": stmt_type,
                        "service_key": svc_key,
                    })
                    stmt_svc_map.append(svc_key)

        if statements:
            try:
                session = _get_session()
                raw = session.sql(
                    "CALL GRE_APPS.HOL_COMMANDER_APP.EXECUTE_REMOTE_SQL_BATCH(?, ?, ?, ?, ?, ?, ?)",
                    params=[
                        account["conn_account"],
                        DEFAULT_SERVICE_USER,
                        "ACCOUNTADMIN",
                        "COMPUTE_WH",
                        DEFAULT_KEY_SECRET,
                        DEFAULT_PASS_SECRET,
                        json.dumps(statements),
                    ],
                ).collect()
                batch_data = json.loads(raw[0][0])
                if isinstance(batch_data, str):
                    batch_data = json.loads(batch_data)

                if not batch_data.get("login_success"):
                    result["error"] = batch_data.get("error", "Login failed")
                    for svc_key in sql_configs:
                        result["services"][svc_key] = {"success": False, "error": None}
                    all_ok = False
                else:
                    # Map batch results back to service keys
                    svc_stmt_results: Dict[str, list] = {}
                    for br in batch_data.get("results", []):
                        sk = br.get("service_key", "")
                        if sk not in svc_stmt_results:
                            svc_stmt_results[sk] = []
                        svc_stmt_results[sk].append(br)

                    for svc_key, cfg in sql_configs.items():
                        svc_results_list = svc_stmt_results.get(svc_key, [])
                        svc_result = {"success": False, "error": None}

                        if cfg["service_type"] == "dynamic_action":
                            if svc_results_list:
                                dr = svc_results_list[0]
                                svc_result["success"] = dr.get("success", False)
                                svc_result["error"] = dr.get("error")
                            else:
                                svc_result["error"] = "No result returned"

                        elif cfg["service_type"] in ("custom_sql", "output"):
                            stmt_results = []
                            for br in svc_results_list:
                                sr = {
                                    "sql": br.get("sql", statements[br["index"]]["sql"] if br.get("index") is not None and br["index"] < len(statements) else ""),
                                    "success": br.get("success", False),
                                    "error": br.get("error"),
                                    "columns": br.get("columns"),
                                    "rows": br.get("rows"),
                                }
                                stmt_results.append(sr)
                            svc_result["statement_results"] = stmt_results
                            svc_result["success"] = all(s["success"] for s in stmt_results) if stmt_results else False

                        else:
                            # action type
                            svc_result["success"] = all(
                                br.get("success", False) for br in svc_results_list
                            ) if svc_results_list else False
                            errors = [br.get("error") for br in svc_results_list if br.get("error")]
                            if errors:
                                svc_result["error"] = "; ".join(errors)

                        if not svc_result["success"]:
                            all_ok = False
                        result["services"][svc_key] = svc_result

            except Exception as e:
                result["error"] = str(e)
                for svc_key in sql_configs:
                    result["services"][svc_key] = {"success": False, "error": None}
                all_ok = False

    result["success"] = all_ok
    return result


def _circle_for_result(result: Dict) -> str:
    if result["success"]:
        has_warning = any(not s.get("success") for s in result.get("services", {}).values())
        return "🟡" if has_warning else "🟢"
    return "🔴"


def _run_apply_job(
    accounts: List[Dict],
    service_configs: Dict[str, Dict],
    parallel: bool,
    max_workers: int,
    api_client: Optional[DataOpsClient] = None,
    event_slug: Optional[str] = None,
):
    order = [acc["account_id"] for acc in accounts]
    total = len(accounts)
    done_map: Dict = {}

    def _record(r):
        done_map[r["account_id"]] = r
        circles = [
            _circle_for_result(done_map[aid]) if aid in done_map else "⚪"
            for aid in order
        ]
        with _apply_lock:
            _apply_job["results"].append(r)
            _apply_job["completed"] = len(done_map)
            _apply_job["circles"] = circles

    try:
        if parallel:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_acc = {
                    executor.submit(
                        _run_services_core, acc, service_configs, api_client, event_slug
                    ): acc
                    for acc in accounts
                }
                for future in as_completed(future_to_acc):
                    if _apply_job["cancel"].is_set():
                        for f in future_to_acc:
                            f.cancel()
                        break
                    r = future.result()
                    _record(r)
                    with _apply_lock:
                        _apply_job["status"] = (
                            f"Completed {len(done_map)}/{total} (~{max_workers} concurrent)..."
                        )
        else:
            for acc in accounts:
                if _apply_job["cancel"].is_set():
                    break
                with _apply_lock:
                    _apply_job["status"] = (
                        f"Applying to {acc['suffix']} ({len(done_map) + 1}/{total})..."
                    )
                r = _run_services_core(acc, service_configs, api_client, event_slug)
                _record(r)
    finally:
        with _apply_lock:
            _apply_job["running"] = False
            _apply_job["cancelled"] = _apply_job["cancel"].is_set()
            _apply_job["status"] = "Cancelled." if _apply_job["cancelled"] else "Done!"


# =============================================================================
# Sidebar — DataOps API connection (auto-connects via secret)
# =============================================================================

with st.sidebar:
    st.header("🔑 DataOps API")

    # Check if token is available in secrets
    _has_token = bool(_get_secret('DATAOPS_API_TOKEN'))

    if _has_token:
        client = DataOpsClient()
        try:
            client.health_check()
            st.session_state.dataops_connected = True
            st.session_state.dataops_auth_method = client.auth_method
            st.success(f"Connected ({client.auth_method.upper()})")
        except Exception as e:
            st.session_state.dataops_connected = False
            st.error(f"Connection error: {e}")
            client = None
    else:
        client = None
        st.session_state.dataops_connected = False
        st.warning("DATAOPS_API_TOKEN secret not populated")

    st.divider()
    st.caption("Token is stored as a Snowflake secret. Update it via ALTER SECRET.")

# =============================================================================
# Main UI
# =============================================================================

st.title("DataOps.live Hands-On Lab Commander")
st.caption("Apply administrative operations across Snowflake hands-on lab accounts")

if not _has_token:
    with st.container():
        st.warning(
            "🔑 **DataOps API token not configured** — "
            "Populate the DATAOPS_API_TOKEN secret in GRE_APPS.HOL_COMMANDER_APP.",
        )

# =============================================================================
# Section 1: Event Selection
# =============================================================================

st.subheader("📅 Event selection")

if client and st.session_state.dataops_connected:
    # --- Quick Access ---
    pinned = get_pinned_events()
    st.caption("**Quick access**")
    pin_cols = st.columns(min(len(pinned) + 1, 6))
    for i, event in enumerate(pinned):
        col = pin_cols[i % len(pin_cols)]
        with col:
            is_hardcoded = event["slug"] in [e["slug"] for e in HARDCODED_EVENTS]
            btn_label = f"⚡ {event['name']}" if is_hardcoded else f"⭐ {event['name']}"
            if st.button(btn_label, key=f"pin_{event['slug']}", use_container_width=True):
                st.session_state.selected_event_slug = event["slug"]
                st.session_state.api_accounts = []
                st.session_state.api_accounts_raw = []
                st.session_state.account_source_events = {}
                st.session_state.pop("event_filter_state", None)
                _rerun()
            if not is_hardcoded:
                if st.button("❌", key=f"unpin_{event['slug']}", help="Remove from quick access"):
                    remove_favorite(event["slug"])
                    _rerun()

    # --- Event Search ---
    st.caption("**Search events**")
    event_search_query = st.text_input(
        "Search events",
        placeholder="Search by event name...",
        label_visibility="collapsed",
        key="event_search_input",
    )

    if event_search_query != st.session_state.get("_last_event_search", ""):
        st.session_state._last_event_search = event_search_query
        if event_search_query:
            try:
                results = client.get_events(search=event_search_query)
                if isinstance(results, dict) and "items" in results:
                    st.session_state.event_search_results = results["items"]
                elif isinstance(results, dict) and "events" in results:
                    st.session_state.event_search_results = results["events"]
                elif isinstance(results, list):
                    st.session_state.event_search_results = results
                else:
                    st.session_state.event_search_results = []
            except Exception as e:
                st.error(f"Search failed: {e}")
                st.session_state.event_search_results = []
        else:
            st.session_state.event_search_results = []

    if st.session_state.event_search_results:
        def _is_decommissioned_event(evt):
            pool = evt.get("account_pool")
            if isinstance(pool, dict) and "decommission" in (pool.get("status", "") or "").lower():
                return True
            if evt.get("decommission_datetime"):
                from datetime import datetime, timezone
                try:
                    dt = datetime.fromisoformat(evt["decommission_datetime"].replace("Z", "+00:00"))
                    if dt < datetime.now(timezone.utc):
                        return True
                except (ValueError, TypeError):
                    pass
            return False

        active_results = [
            evt for evt in st.session_state.event_search_results
            if not _is_decommissioned_event(evt)
        ]
        show_results = bool(active_results) and not st.session_state.selected_event_slug and not st.session_state.selected_accounts
        with st.expander(f"Search results ({len(active_results)})", expanded=show_results):
            for idx, evt in enumerate(active_results):
                evt_name = evt.get("name", "Unknown")
                evt_slug = evt.get("slug", "")
                evt_location = evt.get("location", "")
                display = f"**{evt_name}** ({evt_slug})"
                if evt_location:
                    display += f" -- {evt_location}"

                c1, c2, c3, c4 = st.columns([6, 1, 1, 1])
                with c1:
                    st.markdown(display)
                with c2:
                    if st.button("✅", key=f"select_evt_{idx}", help="Select this event"):
                        st.session_state.selected_event_slug = evt_slug
                        st.session_state.api_accounts = []
                        st.session_state.api_accounts_raw = []
                        st.session_state.account_source_events = {}
                        st.session_state.pop("event_filter_state", None)
                        _rerun()
                with c3:
                    if st.button("➕", key=f"merge_evt_{idx}", help="Add accounts from this event"):
                        st.session_state._merge_event_slug = evt_slug
                        st.session_state._merge_event_name = evt_name
                        _rerun()
                with c4:
                    if st.button("⭐", key=f"fav_evt_{idx}", help="Pin to quick access"):
                        add_favorite(evt_slug, evt_name)
                        _rerun()

        with st.expander("🐛 Debug: Raw event search results", expanded=False):
            st.json(st.session_state.event_search_results[:5])

    # --- Merge accounts from another event ---
    if st.session_state.get("_merge_event_slug"):
        merge_slug = st.session_state._merge_event_slug
        merge_name = st.session_state.get("_merge_event_name", merge_slug)
        try:
            with st.spinner(f"Fetching accounts from **{merge_name}**..."):
                merge_raw = client.get_all_event_accounts(merge_slug)
            merge_accounts = [api_account_to_internal(a) for a in merge_raw]
            existing_ids = {a["account_id"] for a in st.session_state.api_accounts}
            new_accounts = [a for a in merge_accounts if a["account_id"] not in existing_ids]
            new_raw = [r for r in merge_raw if api_account_to_internal(r)["account_id"] not in existing_ids]
            st.session_state.api_accounts.extend(new_accounts)
            st.session_state.api_accounts_raw.extend(new_raw)
            for acc in new_accounts:
                st.session_state.account_source_events[acc["account_id"]] = merge_slug
            if new_accounts:
                st.success(
                    f"Added **{len(new_accounts)}** account(s) from **{merge_name}** "
                    f"({len(merge_accounts) - len(new_accounts)} duplicate(s) skipped)",
                )
            else:
                st.info(
                    f"All **{len(merge_accounts)}** account(s) from **{merge_name}** were already in the pool",
                )
        except Exception as e:
            st.error(f"Failed to fetch accounts from {merge_name}: {e}")
        finally:
            st.session_state._merge_event_slug = None
            st.session_state._merge_event_name = None

    # --- Load accounts for selected event ---
    if st.session_state.selected_event_slug:
        st.divider()
        st.markdown(f"### 📅 `{st.session_state.selected_event_slug}`")

        if not st.session_state.api_accounts:
            try:
                with st.spinner("Fetching all accounts (paginating)..."):
                    raw_accounts = client.get_all_event_accounts(st.session_state.selected_event_slug)
                st.session_state.api_accounts_raw = raw_accounts
                st.session_state.api_accounts = [api_account_to_internal(a) for a in raw_accounts]
                for acc in st.session_state.api_accounts:
                    st.session_state.account_source_events[acc["account_id"]] = st.session_state.selected_event_slug
            except Exception as e:
                st.session_state["_api_load_error"] = str(e)
                st.error(f"Failed to load accounts: {e}")
            else:
                st.session_state.pop("_api_load_error", None)

        st.checkbox(
            "Hide decommissioned accounts",
            value=True,
            key="hide_decommissioned",
            help="Filter out accounts with status containing 'decommission'",
        )

        visible_api_accounts = st.session_state.api_accounts
        if st.session_state.hide_decommissioned and visible_api_accounts:
            visible_api_accounts = [
                a for a in visible_api_accounts
                if "decommission" not in (a.get("status", "") or "").lower()
            ]
            filtered_count = len(st.session_state.api_accounts) - len(visible_api_accounts)
            if filtered_count > 0:
                st.caption(f"🔍 Hiding **{filtered_count}** decommissioned account(s)")

        st.session_state["_visible_api_accounts"] = visible_api_accounts

        if st.session_state.api_accounts:
            st.success(
                f"Loaded **{len(st.session_state.api_accounts)}** total account(s) from API "
                f"(**{len(visible_api_accounts)}** active)",
            )
        elif st.session_state.api_accounts_raw is not None:
            st.warning("No accounts found for this event")

        with st.expander("🐛 Debug: Raw API response", expanded=False):
            slug_under_test = st.session_state.selected_event_slug
            st.caption(f"Event slug: `{slug_under_test}`")
            if st.button("Test API call (show raw response)", key="test_api_call"):
                try:
                    raw_resp = client.get_event_accounts(slug_under_test, page=1, page_size=5)
                    st.json(raw_resp)
                except Exception as e:
                    st.error(f"API error: {e}")
            if st.session_state.get("_api_load_error"):
                st.error(f"Last load error: {st.session_state['_api_load_error']}")
            if st.session_state.api_accounts_raw:
                st.caption(f"Total accounts fetched: **{len(st.session_state.api_accounts_raw)}**")
                st.json(st.session_state.api_accounts_raw[:3])

        if st.button("🔄 Reload accounts", key="reload_accounts"):
            st.session_state.api_accounts = []
            st.session_state.api_accounts_raw = []
            st.session_state.account_source_events = {}
            st.session_state.pop("event_filter_state", None)
            _rerun()

        st.divider()
        st.caption("**Event actions**")
        if st.button("🔄 Re-run configure pipeline", key="rerun_pipeline_btn", use_container_width=True):
            st.session_state._confirm_rerun_pipeline = True

        if st.session_state.get("_confirm_rerun_pipeline"):
            st.warning(
                f"This will re-run the configure pipeline for **{st.session_state.selected_event_slug}**.",
            )
            c1, c2 = st.columns(2)
            with c1:
                if st.button("✅ Confirm", key="confirm_rerun", type="primary", use_container_width=True):
                    st.session_state._confirm_rerun_pipeline = False
                    try:
                        client.rerun_configure_pipeline(st.session_state.selected_event_slug)
                        st.success("Pipeline triggered successfully!")
                    except Exception as e:
                        st.error(f"Failed: {e}")
            with c2:
                if st.button("❌ Cancel", key="cancel_rerun", use_container_width=True):
                    st.session_state._confirm_rerun_pipeline = False
                    _rerun()

        if st.button("🗑️ Clear event selection", key="clear_event"):
            st.session_state.selected_event_slug = None
            st.session_state.api_accounts = []
            st.session_state.api_accounts_raw = []
            st.session_state.event_search_results = []
            st.session_state.account_source_events = {}
            _rerun()

else:
    st.info("Populate the DATAOPS_API_TOKEN secret to enable event search.")

# =============================================================================
# Section 1b: CSV Fallback
# =============================================================================

with st.expander("📄 Manual CSV input (fallback)", expanded=not st.session_state.dataops_connected):
    csv_input = st.text_area(
        "Account CSV",
        height=150,
        placeholder="""Account ID,Status,Assigned To,URL
MAKE_YOUR_DATA_AI_READY_RETAIL_DBSFEV,ready,user@snowflake.com,https://...""",
        help="Paste CSV data from the Preparation tab of your event"
    )
    csv_accounts = parse_account_csv(csv_input) if csv_input else []
    if csv_accounts:
        st.success(f"Parsed **{len(csv_accounts)}** account(s) from CSV")

# --- Determine active account source ---
if st.session_state.get("_visible_api_accounts"):
    accounts = st.session_state["_visible_api_accounts"]
    account_source = "api"
elif st.session_state.api_accounts:
    accounts = st.session_state.api_accounts
    account_source = "api"
elif csv_accounts:
    accounts = csv_accounts
    account_source = "csv"
else:
    accounts = []
    account_source = None

# =============================================================================
# Section 2: Account selection
# =============================================================================

if accounts:
    all_account_ids = {acc["account_id"] for acc in accounts}

    needs_init = (
        "selected_accounts_initialized" not in st.session_state
        or st.session_state.get("last_account_ids") != all_account_ids
    )
    if needs_init:
        st.session_state.selected_accounts = set()
        st.session_state.selected_accounts_initialized = True
        st.session_state.last_account_ids = all_account_ids
        for acc in accounts:
            st.session_state[f"acc_{acc['account_id']}"] = False
        if len(accounts) > 1:
            st.session_state.parallel_execution = True
            st.session_state.parallel_workers = 5

    source_label = "API" if account_source == "api" else "CSV"
    st.caption(f"ℹ️ **{len(accounts)}** account(s) loaded from {source_label}")

    search_col, clear_col = st.columns([11, 1])
    with search_col:
        email_search = st.text_input(
            "Search by email",
            key=f"email_search_{st.session_state.search_clear_count}",
            placeholder="Search by email  e.g. cameron shimmin",
            help="Fuzzy search on the assigned email",
            label_visibility="collapsed",
        )
    with clear_col:
        if email_search:
            if st.button("X", key="clear_search", help="Clear search", use_container_width=True):
                st.session_state.search_clear_count += 1
                _rerun()

    # --- Event source filter ---
    source_events = st.session_state.get("account_source_events", {})
    unique_events = sorted(set(source_events.values()))
    if unique_events:
        st.caption("**Events in pool**")
        with st.container():
            st.session_state.setdefault("event_filter_state", {})
            for evt in unique_events:
                if evt not in st.session_state.event_filter_state:
                    st.session_state.event_filter_state[evt] = True

            evt_cols = st.columns(min(len(unique_events), 4))
            for i, evt in enumerate(unique_events):
                with evt_cols[i % min(len(unique_events), 4)]:
                    count = sum(1 for aid, e in source_events.items() if e == evt)
                    st.checkbox(
                        f"{evt} ({count})",
                        value=st.session_state.event_filter_state.get(evt, True),
                        key=f"evt_filter_{evt}",
                    )
                    st.session_state.event_filter_state[evt] = st.session_state[f"evt_filter_{evt}"]

        active_events = {e for e in unique_events if st.session_state.event_filter_state.get(e, True)}
        visible_accounts = [
            acc for acc in accounts
            if fuzzy_match(email_search, acc["assigned_to"])
            and source_events.get(acc["account_id"], "") in active_events
        ]
    else:
        visible_accounts = [acc for acc in accounts if fuzzy_match(email_search, acc["assigned_to"])]

    col_sel_all, col_sel_none = st.columns(2)
    with col_sel_all:
        if st.button("Select all visible", use_container_width=True):
            for acc in visible_accounts:
                st.session_state.selected_accounts.add(acc["account_id"])
                st.session_state[f"acc_{acc['account_id']}"] = True
            _rerun()
    with col_sel_none:
        if st.button("Select none", use_container_width=True):
            st.session_state.selected_accounts = set()
            for acc in accounts:
                st.session_state[f"acc_{acc['account_id']}"] = False
            _rerun()

    _results_by_account = {}
    for r in st.session_state.results:
        _results_by_account[r["account_id"]] = r

    selected_count = len(st.session_state.selected_accounts)
    with st.expander(f"👥 Select accounts ({selected_count}/{len(visible_accounts)} selected)", expanded=True):
        if not visible_accounts:
            st.caption("No accounts match the search.")
        for acc in visible_accounts:
            is_selected = acc["account_id"] in st.session_state.selected_accounts
            status_icon = ""
            acc_result = _results_by_account.get(acc["account_id"])
            if acc_result is not None:
                if acc_result["success"]:
                    has_warning = any(
                        not svc_r.get("success")
                        for svc_r in acc_result.get("services", {}).values()
                    )
                    status_icon = "🟡 " if has_warning else "🟢 "
                else:
                    status_icon = "🔴 "
            assigned_display = (
                f"{acc['assigned_to'][:40]}..."
                if len(acc["assigned_to"]) > 40
                else acc["assigned_to"]
            )
            url_part = f" -- [{acc['url']}]({acc['url']})" if acc.get("url") else ""
            display_name = f"{acc['suffix']}** -- {assigned_display}{url_part}"
            label = f"{status_icon}**{display_name}"
            if st.checkbox(label, value=is_selected, key=f"acc_{acc['account_id']}"):
                st.session_state.selected_accounts.add(acc["account_id"])
            else:
                st.session_state.selected_accounts.discard(acc["account_id"])

    if not st.session_state.selected_accounts:
        st.warning("No accounts selected")

# =============================================================================
# Section 3: Admin services
# =============================================================================

st.subheader("🛠️ Admin services")
st.caption("Select the operations to apply to every selected account")

# In SiS, only key-pair auth is supported (via REST API sproc)
st.info("🔑 Using key-pair auth via EMERGENCY_SERVICE_USER (REST API)")

# Target user selector
if st.session_state.get("target_user") not in ("USER", "ADMIN", "Custom"):
    st.session_state["target_user"] = "USER"
st.radio(
    "Target user",
    options=["USER", "ADMIN", "Custom"],
    key="target_user",
    horizontal=True,
    help="Which Snowflake user to target for service operations (e.g. password reset, MFA bypass)",
)
if st.session_state.get("target_user") == "Custom":
    st.text_input(
        "Custom username",
        key="custom_target_user",
        placeholder="e.g. JOHN_DOE",
        help="Enter the Snowflake username to target",
    )

selected_services = []

with st.container():
    rendered_groups = set()
    for svc_key, svc in SERVICES.items():
        group_id = svc.get("group")

        if group_id and group_id not in rendered_groups:
            rendered_groups.add(group_id)
            grp = SERVICE_GROUPS.get(group_id, {})
            group_svcs = [(k, v) for k, v in SERVICES.items() if v.get("group") == group_id]
            with st.container():
                st.markdown(f"{grp.get('icon', '')} **{grp.get('label', group_id)}**")
                st.caption(grp.get("description", ""))
                for g_key, g_svc in group_svcs:
                    col1, col2 = st.columns([1, 8])
                    with col1:
                        g_checked = st.checkbox(
                            "enable",
                            key=f"svc_{g_key}",
                            value=g_key in st.session_state.active_services,
                            label_visibility="collapsed"
                        )
                    with col2:
                        st.markdown(f"{g_svc['icon']} **{g_svc['label']}**")
                        st.caption(g_svc["description"])
                        if "render_config" in g_svc:
                            g_svc["render_config"]()
                        if "get_preview" in g_svc:
                            with st.expander("View SQL"):
                                st.code(g_svc["get_preview"](), language="sql")
                    if g_checked:
                        selected_services.append(g_key)
                        st.session_state.active_services.add(g_key)
                    else:
                        st.session_state.active_services.discard(g_key)
            st.markdown("---")
            continue

        if group_id:
            continue

        col1, col2 = st.columns([1, 8])
        with col1:
            checked = st.checkbox(
                "enable",
                key=f"svc_{svc_key}",
                value=svc_key in st.session_state.active_services,
                label_visibility="collapsed"
            )
        with col2:
            st.markdown(f"{svc['icon']} **{svc['label']}**")
            st.caption(svc["description"])
            if "render_config" in svc:
                svc["render_config"]()
            if "get_preview" in svc:
                with st.expander("View SQL"):
                    st.code(svc["get_preview"](), language="sql")

        if checked:
            selected_services.append(svc_key)
            st.session_state.active_services.add(svc_key)
        else:
            st.session_state.active_services.discard(svc_key)
        st.markdown("---")

# =============================================================================
# Section 4: Apply
# =============================================================================

st.subheader("▶️ Apply")

selected_accounts_list = [acc for acc in accounts if acc["account_id"] in st.session_state.selected_accounts]
can_apply = bool(selected_accounts_list and selected_services)

if not accounts:
    st.caption("Select an event above or paste CSV data to get started.")
elif not selected_accounts_list:
    st.warning("Select at least one account to continue.")
elif not selected_services:
    st.warning("Select at least one service to apply.")
else:
    svc_labels = [SERVICES[k]["label"].replace("`", "") for k in selected_services]
    st.caption(
        f"Will run **{len(selected_services)}** service(s) on **{len(selected_accounts_list)}** account(s): "
        + ", ".join(svc_labels)
    )

# --- Parallel execution controls ---
par_col, workers_col = st.columns([3, 2])
with par_col:
    parallel_enabled = st.checkbox(
        "⚡ Parallel execution",
        key="parallel_execution",
        help="Run accounts concurrently.",
    )
with workers_col:
    if parallel_enabled:
        st.number_input(
            "Max concurrent",
            min_value=2,
            max_value=20,
            step=1,
            key="parallel_workers",
            help="Maximum number of accounts processed simultaneously.",
        )

if st.button(
    f"🚀 Apply to {len(selected_accounts_list)} account(s)"
    if selected_accounts_list
    else "🚀 Apply",
    type="primary",
    disabled=not can_apply or _apply_job["running"],
    use_container_width=True,
):
    parallel = st.session_state.get("parallel_execution", False)
    max_workers = st.session_state.get("parallel_workers", 5)
    service_configs = resolve_service_configs(selected_services)

    with _apply_lock:
        _apply_job["cancel"].clear()
        _apply_job["running"] = True
        _apply_job["cancelled"] = False
        _apply_job["results"] = []
        _apply_job["circles"] = ["⚪"] * len(selected_accounts_list)
        _apply_job["completed"] = 0
        _apply_job["total"] = len(selected_accounts_list)
        _apply_job["status"] = "Starting..."

    st.session_state.results = []
    st.session_state._apply_accounts = selected_accounts_list
    st.session_state._collecting_apply_results = True

    threading.Thread(
        target=_run_apply_job,
        args=(selected_accounts_list, service_configs, parallel, max_workers, client, st.session_state.get("selected_event_slug")),
        daemon=True,
    ).start()
    _rerun()

# --- Live progress + cancel ---
if _apply_job["running"]:
    with _apply_lock:
        circles = list(_apply_job["circles"])
        completed = _apply_job["completed"]
        total_j = _apply_job["total"]
        status = _apply_job["status"]

    st.markdown(" ".join(circles) if circles else "")
    st.progress(completed / total_j if total_j > 0 else 0)
    st.text(status)

    if st.button(
        "⏹️ Cancel",
        key="cancel_apply_btn",
        type="secondary",
        use_container_width=True,
    ):
        _apply_job["cancel"].set()

    time.sleep(0.4)
    _rerun()

elif st.session_state.get("_collecting_apply_results"):
    with _apply_lock:
        finished_results = list(_apply_job["results"])
    st.session_state.results = finished_results
    st.session_state._collecting_apply_results = False
    _rerun()

# =============================================================================
# Section 5: Results
# =============================================================================

if st.session_state.results:
    st.subheader("📊 Results")

    success_count = sum(1 for r in st.session_state.results if r["success"])
    fail_count = len(st.session_state.results) - success_count

    col1, col2 = st.columns(2)
    with col1:
        st.metric("Successful", success_count)
    with col2:
        st.metric("Failed", fail_count)

    if st.session_state.get("results_filter") not in ("All", "Success", "Failure"):
        st.session_state["results_filter"] = "All"

    st.radio(
        "Show",
        options=["All", "Success", "Failure"],
        key="results_filter",
        horizontal=True,
    )

    results_filter = st.session_state.get("results_filter") or "All"
    if results_filter == "Success":
        filtered_results = [r for r in st.session_state.results if r["success"]]
    elif results_filter == "Failure":
        filtered_results = [r for r in st.session_state.results if not r["success"]]
    else:
        filtered_results = st.session_state.results

    # --- Aggregated output panels for "output" services ---
    output_services = {k: v for k, v in SERVICES.items() if v.get("service_type") == "output"}
    for svc_key, svc in output_services.items():
        output_col = svc.get("output_column")
        values = []
        failures = []
        for result in st.session_state.results:
            svc_result = result.get("services", {}).get(svc_key)
            if not svc_result:
                if result.get("error"):
                    failures.append({"suffix": result["suffix"], "error": result["error"]})
                continue
            if svc_result.get("success"):
                for sr in svc_result.get("statement_results", []):
                    if sr.get("success") and sr.get("columns") and sr.get("rows"):
                        if output_col and output_col in sr["columns"]:
                            col_idx = sr["columns"].index(output_col)
                            for row in sr["rows"]:
                                values.append(str(row[col_idx]))
                        else:
                            for row in sr["rows"]:
                                values.append(str(row[0]))
            else:
                error = svc_result.get("error") or "Unknown error"
                failures.append({"suffix": result["suffix"], "error": error})

        if values or failures:
            with st.expander(
                f"{svc['icon']} **{svc['label']}** ({len(values)} collected{f', {len(failures)} failed' if failures else ''})",
                expanded=False,
            ):
                if values:
                    st.code("\n".join(values), language=None)
                if failures:
                    st.caption(f"**{len(failures)} failed:**")
                    for f in failures:
                        st.caption(f"{f['suffix']}: {f['error']}")

    for result in filtered_results:
        if result["success"]:
            icon = "✅"
            status = "Success"
        else:
            icon = "❌"
            status = "Failed"

        with st.expander(
            f"{icon} **{result['suffix']}** {result.get('assigned_to', '')}"
            + (f" -- {result['url']}" if result.get("url") else "")
            + f" -- {status}",
            expanded=not result["success"],
        ):
            if result["error"]:
                st.error(f"Connection error: {result['error']}")
            else:
                for svc_key, svc_result in result["services"].items():
                    svc = SERVICES[svc_key]
                    if svc.get("service_type") == "output":
                        if svc_result.get("success"):
                            st.markdown(f":green-badge[OK] {svc['icon']} {svc['label']}")
                        else:
                            st.markdown(f":red-badge[Failed] {svc['icon']} {svc['label']}")
                            if svc_result.get("error"):
                                st.caption(f"Error: {svc_result['error']}")
                        continue
                    if svc.get("service_type") == "custom_sql":
                        stmt_results = svc_result.get("statement_results", [])
                        if stmt_results:
                            ok = sum(1 for s in stmt_results if s["success"])
                            fail = len(stmt_results) - ok
                            if fail == 0:
                                st.markdown(f":green-badge[OK] {svc['icon']} {svc['label']} ({ok}/{len(stmt_results)} passed)")
                            else:
                                st.markdown(f":red-badge[{fail} failed] {svc['icon']} {svc['label']} ({ok}/{len(stmt_results)} passed)")
                            for j, sr in enumerate(stmt_results):
                                status_sr = "OK" if sr["success"] else "FAIL"
                                preview = sr.get("sql", "").replace('\n', ' ').strip()[:60]
                                st.markdown(f"**[{status_sr}]** `{preview}`")
                                if sr.get("error"):
                                    st.error(sr["error"])
                                if sr["success"] and sr.get("columns") and sr.get("rows"):
                                    df = pd.DataFrame(sr["rows"], columns=sr["columns"])
                                    st.dataframe(df, use_container_width=True)
                                elif sr["success"]:
                                    st.caption("Executed successfully (no result set)")
                        elif svc_result.get("error"):
                            st.error(f"Custom SQL error: {svc_result['error']}")
                    else:
                        if svc_result["success"]:
                            st.markdown(f":green-badge[OK] {svc['icon']} {svc['label']}")
                        else:
                            st.markdown(f":red-badge[Failed] {svc['icon']} {svc['label']}")
                            if svc_result.get("error"):
                                st.caption(f"Error: {svc_result['error']}")

# =============================================================================
# Footer
# =============================================================================

st.caption("HOL Commander v2 | Streamlit-in-Snowflake | Key-pair auth via REST API")
