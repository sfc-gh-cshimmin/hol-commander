"""
Snowflake Intelligence Launchpad - Support Admin v2
Streamlit app with DataOps.live API integration for automated event/account management
"""

import os
import re
import json
import time
import logging
import pathlib
import threading
import pandas as pd
import requests
import streamlit as st
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional

logging.basicConfig(
    filename=pathlib.Path.home() / "si_admin_v2.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

logger.info("App script executed (Streamlit rerun)")


# --- App-level password gate ---
def _check_app_password():
    """Block access until the user provides the correct app password."""
    app_password = st.secrets.get("APP_PASSWORD", "")
    if not app_password:
        return  # No password configured — open access

    if st.session_state.get("app_authenticated"):
        return

    st.title("🔒 Login Required")
    entered = st.text_input("Password", type="password", key="_app_pw_input")
    if st.button("Enter"):
        if entered == app_password:
            st.session_state["app_authenticated"] = True
            st.rerun()
        else:
            st.error("Incorrect password.")
    st.stop()


_check_app_password()


FAVORITES_PATH = pathlib.Path.home() / ".si_admin_favorites.json"


def _hol_password() -> str:
    """Return the HOL default password from Streamlit secrets."""
    return st.secrets.get("HOL_PASSWORD", "")


def _hol_temp_password() -> str:
    """Return the HOL intermediate temp password from Streamlit secrets."""
    return st.secrets.get("HOL_TEMP_PASSWORD", "")


# --- Key-pair auth configuration ---
DEFAULT_KEY_PATH = os.path.expanduser(
    st.secrets.get("PRIVATE_KEY_PATH", "~/SI_ACCOUNTS_PEM_KEY/priv_key.pem")
)
DEFAULT_SERVICE_USER = st.secrets.get("SERVICE_USER", "EMERGENCY_SERVICE_USER")
_PRIVATE_KEY_PEM_SECRET = st.secrets.get("PRIVATE_KEY_PEM", "")


@st.cache_resource
def _load_private_key_from_pem(pem_content: str) -> Optional[bytes]:
    """Parse PEM content string and return DER-encoded private key bytes."""
    try:
        private_key = serialization.load_pem_private_key(
            pem_content.encode(), password=None, backend=default_backend()
        )
        return private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    except Exception as e:
        logger.error("Failed to parse PEM content from secret: %s", e)
        return None


@st.cache_resource
def _load_private_key(key_path: str) -> Optional[bytes]:
    """Load RSA private key from PEM file and return DER-encoded bytes."""
    try:
        with open(key_path, "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(), password=None, backend=default_backend()
            )
        return private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    except FileNotFoundError:
        logger.warning("Private key file not found: %s", key_path)
        return None
    except Exception as e:
        logger.error("Failed to load private key from %s: %s", key_path, e)
        return None


def _get_private_key_der() -> Optional[bytes]:
    """Resolve private key: prefer PRIVATE_KEY_PEM secret, fall back to file path."""
    if _PRIVATE_KEY_PEM_SECRET:
        return _load_private_key_from_pem(_PRIVATE_KEY_PEM_SECRET)
    key_path = st.session_state.get("private_key_path", DEFAULT_KEY_PATH)
    return _load_private_key(key_path)


HARDCODED_EVENTS = [
    {"slug": "launchpad-industry-demos", "name": "Launchpad Industry Demos"},
]

# Module-level apply job state — cached across reruns via st.cache_resource so
# that the dict (and threading.Event/Lock) are not reset when Streamlit
# re-executes the script on each rerun.
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
# Page configuration
# =============================================================================

st.set_page_config(
    page_title="DataOps.live HOL Commander",
    page_icon=":material/admin_panel_settings:",
    layout="wide"
)

# =============================================================================
# DataOps API Client
# =============================================================================

class DataOpsClient:
    BASE_URL = "https://admin.dataops.live/api/v1"

    def __init__(self, token: str):
        self.token = token
        self.auth_method: Optional[str] = None

    def _headers(self, method: str) -> dict:
        if method == "pat":
            return {"private-token": self.token}
        return {"Authorization": f"Bearer {self.token}"}

    def _get(self, path: str, params: Optional[dict] = None):
        methods_to_try = []
        if self.auth_method:
            methods_to_try.append(self.auth_method)
            other = "bearer" if self.auth_method == "pat" else "pat"
            methods_to_try.append(other)
        else:
            methods_to_try = ["pat", "bearer"]

        last_resp = None
        for method in methods_to_try:
            resp = requests.get(
                f"{self.BASE_URL}{path}",
                headers=self._headers(method),
                params=params,
                timeout=15,
            )
            if resp.status_code not in (401, 403):
                self.auth_method = method
                resp.raise_for_status()
                return resp.json()
            last_resp = resp

        last_resp.raise_for_status()
        return last_resp.json()

    def _post(self, path: str, json_data: Optional[dict] = None, params: Optional[dict] = None):
        methods_to_try = []
        if self.auth_method:
            methods_to_try.append(self.auth_method)
            other = "bearer" if self.auth_method == "pat" else "pat"
            methods_to_try.append(other)
        else:
            methods_to_try = ["pat", "bearer"]

        last_resp = None
        for method in methods_to_try:
            resp = requests.post(
                f"{self.BASE_URL}{path}",
                headers=self._headers(method),
                json=json_data,
                params=params,
                timeout=30,
            )
            if resp.status_code not in (401, 403):
                self.auth_method = method
                resp.raise_for_status()
                return resp.json() if resp.text else {}
            last_resp = resp

        last_resp.raise_for_status()
        return last_resp.json() if last_resp.text else {}

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
        """Decommission an event account via the DataOps API."""
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
# Favorites helpers
# =============================================================================

def load_favorites() -> List[Dict]:
    if FAVORITES_PATH.exists():
        try:
            data = json.loads(FAVORITES_PATH.read_text())
            if isinstance(data, list):
                return data
        except (json.JSONDecodeError, OSError):
            pass
    return []


def save_favorites(favorites: List[Dict]):
    FAVORITES_PATH.write_text(json.dumps(favorites, indent=2))


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
    favorites = load_favorites()
    if not any(f["slug"] == slug for f in favorites):
        favorites.append({"slug": slug, "name": name})
        save_favorites(favorites)


def remove_favorite(slug: str):
    favorites = load_favorites()
    favorites = [f for f in favorites if f["slug"] != slug]
    save_favorites(favorites)


# =============================================================================
# Account mapping
# =============================================================================

def api_account_to_internal(api_acc: dict) -> dict:
    identifier = api_acc.get("identifier", "")
    slug = api_acc.get("slug", "")
    account_id = identifier or slug

    if isinstance(account_id, int):
        account_id = str(account_id)

    email = api_acc.get("allocated_to") or ""
    status = api_acc.get("status", "")
    url = api_acc.get("url", "")

    conn_account = f"sfsehol-{identifier}".lower().replace("_", "-") if identifier else ""

    suffix = identifier.split("_")[-1] if "_" in identifier else slug

    return {
        "account_id": identifier or slug,
        "api_id": api_acc.get("id"),  # numeric DB id for API calls (e.g. decommission)
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
st.session_state.setdefault("run_as", "ADMIN")
st.session_state.setdefault("parallel_execution", False)
st.session_state.setdefault("parallel_workers", 5)
st.session_state.setdefault("disable_mfa_comment", "Disabled for SI event")
st.session_state.setdefault("search_clear_count", 0)
st.session_state.setdefault("selected_event_slug", None)
st.session_state.setdefault("api_accounts", [])
st.session_state.setdefault("api_accounts_raw", [])
st.session_state.setdefault("event_search_results", [])
st.session_state.setdefault("dataops_connected", False)
st.session_state.setdefault("dataops_auth_method", None)

st.session_state.setdefault("active_services", set())
st.session_state.setdefault("decommission_remain_allocated", True)
st.session_state.setdefault("account_source_events", {})  # account_id -> event slug/name


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
        conn_account = f"sfsehol-{account_id}".replace("_", "-")

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


def resolve_connection_credentials() -> tuple:
    target = st.session_state.get("target_user")
    if target == "Custom":
        user = st.session_state.get("custom_target_user", "").strip() or "ADMIN"
        password = st.session_state.get("custom_target_password", "").strip() or _hol_password()
        return user, password
    run_as = st.session_state.get("run_as") or "ADMIN"
    if run_as == "USER":
        return "USER", _hol_password()
    return "ADMIN", _hol_password()


def resolve_connection_role() -> Optional[str]:
    target = st.session_state.get("target_user")
    if target == "Custom":
        return None
    run_as = st.session_state.get("run_as") or "ADMIN"
    return "ACCOUNTADMIN" if run_as == "ADMIN" else None


def resolve_connection_params() -> dict:
    """Return kwargs for snowflake.connector.connect() (minus account/warehouse)."""
    auth_mode = st.session_state.get("auth_mode", "keypair")

    if auth_mode == "keypair":
        service_user = st.session_state.get("service_user", DEFAULT_SERVICE_USER)
        private_key_der = _get_private_key_der()
        if private_key_der is None:
            raise RuntimeError("Cannot load private key (check PRIVATE_KEY_PEM secret or file path)")
        return {"user": service_user, "private_key": private_key_der, "role": "ACCOUNTADMIN"}

    # Password mode — existing logic
    conn_user, conn_password = resolve_connection_credentials()
    conn_role = resolve_connection_role()
    params = {"user": conn_user, "password": conn_password}
    if conn_role:
        params["role"] = conn_role
    return params


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
        """CREATE OR REPLACE PASSWORD POLICY my_policy
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


def get_remove_mfa_method_execute():
    """Returns a cursor-level callable that removes all MFA methods for the target user."""
    target = resolve_target_user()

    def execute(cursor):
        cursor.execute(f"SHOW MFA METHODS FOR USER {target}")
        rows = cursor.fetchall()
        if not rows:
            return
        cols = [d[0].lower() for d in cursor.description]
        name_idx = cols.index("name")
        for row in rows:
            mfa_name = row[name_idx]
            cursor.execute(f"ALTER USER {target} REMOVE MFA METHOD {mfa_name}")

    return execute


def get_remove_mfa_method_preview() -> str:
    target = resolve_target_user()
    return (
        f"SHOW MFA METHODS FOR USER {target};\n\n"
        f"-- Then, for each row in the result:\n"
        f"ALTER USER {target} REMOVE MFA METHOD <name>;"
    )


def get_disable_mfa_ddl_template() -> str:
    comment = st.session_state.get("disable_mfa_comment", "Disabled for SI event")
    return f"ALTER ACCOUNT {{locator}} SET REQUIRE_SNOWSIGHT_MFA=false PARAMETER_COMMENT='{comment}';"


def get_disable_mfa_ddl_preview() -> str:
    return get_disable_mfa_ddl_template().replace("{locator}", "<LOCATOR>")


def render_disable_mfa_ddl_config():
    st.text_input(
        "Parameter comment",
        key="disable_mfa_comment",
        help="Value for PARAMETER_COMMENT on the ALTER ACCOUNT statement",
    )


def generate_disable_mfa_ddl(locators: List[str]) -> str:
    template = get_disable_mfa_ddl_template()
    return "\n".join(template.replace("{locator}", loc) for loc in locators)


def get_disable_mfa_policy_statements() -> List[str]:
    return [
        "USE ROLE ACCOUNTADMIN",
        "CREATE SCHEMA IF NOT EXISTS ADMIN.UTIL",
        "USE SCHEMA ADMIN.UTIL",
        "CREATE AUTHENTICATION POLICY IF NOT EXISTS ADMIN.UTIL.MFA_OVERRIDE_POLICY MFA_ENROLLMENT = 'OPTIONAL'",
        "ALTER ACCOUNT SET AUTHENTICATION POLICY MFA_OVERRIDE_POLICY",
    ]


def get_disable_mfa_policy_preview() -> str:
    return """USE ROLE ACCOUNTADMIN;
USE ADMIN.UTIL;
CREATE AUTHENTICATION POLICY IF NOT EXISTS ADMIN.UTIL.MFA_OVERRIDE_POLICY
    MFA_ENROLLMENT = 'OPTIONAL';
ALTER ACCOUNT SET AUTHENTICATION POLICY MFA_OVERRIDE_POLICY;"""


def get_consumption_queries() -> List[Dict]:
    return [
        {
            "label": "Credit summary (last 30 days)",
            "sql": """
                SELECT
                    ROUND(SUM(CREDITS_USED), 2)             AS TOTAL_CREDITS,
                    ROUND(SUM(CREDITS_USED_COMPUTE), 2)     AS COMPUTE_CREDITS,
                    ROUND(SUM(CREDITS_USED_CLOUD_SERVICES), 2) AS CLOUD_SERVICES_CREDITS
                FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
                WHERE START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
            """,
        },
        {
            "label": "Top warehouses by credits (last 30 days)",
            "sql": """
                SELECT
                    NAME AS WAREHOUSE_NAME,
                    ROUND(SUM(CREDITS_USED), 2) AS CREDITS_USED
                FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
                WHERE START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
                  AND NAME IS NOT NULL
                GROUP BY 1
                ORDER BY 2 DESC
                LIMIT 10
            """,
        },
        {
            "label": "Cortex AI usage (last 30 days)",
            "sql": """
                SELECT *
                FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_USAGE_HISTORY
                WHERE START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
                ORDER BY START_TIME DESC
                LIMIT 500
            """,
        },
        {
            "label": "Query activity (last 7 days)",
            "sql": """
                SELECT
                    COUNT(*)                                        AS TOTAL_QUERIES,
                    SUM(CASE WHEN ERROR_CODE IS NOT NULL THEN 1 ELSE 0 END) AS ERROR_COUNT,
                    ROUND(AVG(TOTAL_ELAPSED_TIME) / 1000, 2)       AS AVG_DURATION_SEC
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                WHERE START_TIME >= DATEADD('day', -7, CURRENT_TIMESTAMP())
            """,
        },
        {
            "label": "Storage (most recent snapshot)",
            "sql": """
                SELECT
                    ROUND(STORAGE_BYTES / POWER(1024, 3), 2)         AS DATABASE_GB,
                    ROUND(STAGE_BYTES / POWER(1024, 3), 2)           AS STAGE_GB,
                    ROUND(FAILSAFE_BYTES / POWER(1024, 3), 2)        AS FAILSAFE_GB,
                    ROUND((STORAGE_BYTES + STAGE_BYTES + FAILSAFE_BYTES) / POWER(1024, 3), 2) AS TOTAL_GB
                FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE
                ORDER BY USAGE_DATE DESC
                LIMIT 1
            """,
        },
    ]




def render_consumption_dashboard(results: List[Dict]):
    metric_results = [
        r for r in results
        if r.get("services", {}).get("consumption_metrics", {}).get("success")
    ]
    if len(metric_results) < 2:
        return

    st.subheader(":material/dashboard: Aggregate dashboard")
    st.caption(f"Combined across **{len(metric_results)}** account(s)")

    total_credits = 0.0
    compute_credits = 0.0
    cloud_credits = 0.0
    total_queries = 0
    total_errors = 0
    avg_durations = []
    total_storage_gb = 0.0
    account_credits_rows = []

    for r in metric_results:
        panels = r["services"]["consumption_metrics"].get("data", [])
        for panel in panels:
            label = panel["label"]
            rows = panel["rows"]
            if not rows:
                continue
            if "Credit summary" in label:
                row = rows[0]
                acc_total = float(row[0] or 0)
                total_credits += acc_total
                compute_credits += float(row[1] or 0)
                cloud_credits += float(row[2] or 0)
                account_credits_rows.append({
                    "Account": r["suffix"],
                    "Email": r.get("assigned_to", ""),
                    "Total credits": acc_total,
                    "Compute credits": float(row[1] or 0),
                    "Cloud services credits": float(row[2] or 0),
                })
            elif "Query activity" in label:
                row = rows[0]
                total_queries += int(row[0] or 0)
                total_errors += int(row[1] or 0)
                if row[2] is not None:
                    avg_durations.append(float(row[2]))
            elif "Storage" in label:
                row = rows[0]
                total_storage_gb += float(row[3] or 0)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total credits (30d)", round(total_credits, 2))
    c2.metric("Compute credits (30d)", round(compute_credits, 2))
    c3.metric("Total queries (7d)", f"{total_queries:,}")
    c4.metric("Total storage (GB)", round(total_storage_gb, 2))

    if total_queries > 0:
        error_rate = round(total_errors / total_queries * 100, 2)
        c1, c2, _ = st.columns(3)
        c1.metric("Total errors (7d)", f"{total_errors:,}")
        c2.metric("Error rate (7d)", f"{error_rate}%")

    if account_credits_rows:
        st.caption("**Credits by account (last 30 days)**")
        df = pd.DataFrame(account_credits_rows).sort_values("Total credits", ascending=False)
        st.dataframe(df, use_container_width=True, hide_index=True)


def render_consumption_results(panels: List[Dict]):
    for panel in panels:
        st.caption(f"**{panel['label']}**")
        rows = panel["rows"]
        cols = panel["columns"]

        if not rows:
            st.caption("_No data available_")
            continue

        label = panel["label"]

        if "Credit summary" in label:
            row = rows[0]
            c1, c2, c3 = st.columns(3)
            c1.metric("Total credits", row[0] if row[0] is not None else 0)
            c2.metric("Compute credits", row[1] if row[1] is not None else 0)
            c3.metric("Cloud services credits", row[2] if row[2] is not None else 0)

        elif "Query activity" in label:
            row = rows[0]
            c1, c2, c3 = st.columns(3)
            c1.metric("Total queries", f"{int(row[0]):,}" if row[0] else 0)
            c2.metric("Errors", f"{int(row[1]):,}" if row[1] else 0)
            c3.metric("Avg duration (sec)", row[2] if row[2] is not None else 0)

        elif "Storage" in label:
            row = rows[0]
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Database (GB)", row[0] if row[0] is not None else 0)
            c2.metric("Stage (GB)", row[1] if row[1] is not None else 0)
            c3.metric("Failsafe (GB)", row[2] if row[2] is not None else 0)
            c4.metric("Total (GB)", row[3] if row[3] is not None else 0)

        else:
            df = pd.DataFrame(rows, columns=cols)
            st.dataframe(df, use_container_width=True, hide_index=True)


def parse_sql_statements(sql_text: str) -> List[str]:
    sql_text = re.sub(r'```\w*\n?', '', sql_text).strip()
    raw_statements = sql_text.split(';')
    statements = []
    for stmt in raw_statements:
        stmt = stmt.strip()
        if not stmt:
            continue
        lines = [l.strip() for l in stmt.split('\n') if l.strip() and not l.strip().startswith('--')]
        if lines:
            statements.append(stmt)
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
            help="Enter one or more SQL statements separated by semicolons. Each statement runs sequentially on every selected account.",
            disabled=not enabled,
            label_visibility="collapsed",
        )

    if st.button(":material/add: Add SQL block", key="add_sql_block"):
        st.session_state["custom_sql_block_count"] = num_blocks + 1
        st.rerun()

    if num_blocks > 1:
        if st.button(":material/remove: Remove last block", key="remove_sql_block"):
            # Clear the last block's content and enabled state
            last_key = f"custom_sql_input_{num_blocks - 1}"
            if last_key in st.session_state:
                del st.session_state[last_key]
            enabled_key = f"custom_sql_block_enabled_{num_blocks - 1}"
            if enabled_key in st.session_state:
                del st.session_state[enabled_key]
            st.session_state["custom_sql_block_count"] = num_blocks - 1
            st.rerun()

    stmts = get_custom_sql_statements()
    if stmts:
        st.caption(f":material/code: **{len(stmts)}** statement(s) will be executed per account")


# --- Decommission service helpers ---

def render_decommission_config():
    st.checkbox(
        "Keep account allocated after decommission",
        key="decommission_remain_allocated",
        help="If checked, the account remains allocated to the user but is decommissioned. If unchecked, the account is also deallocated.",
    )


def get_decommission_preview() -> str:
    remain = st.session_state.get("decommission_remain_allocated", True)
    return f"POST /event_management/events/{{event_slug}}/accounts/{{id}}/decommission?remain_allocated={str(remain).lower()}"


def execute_decommission(client: "DataOpsClient", event_slug: str, account: Dict, config: Dict = None) -> Dict:
    """Execute decommission API call for a single account."""
    remain = config.get("remain_allocated", True) if config else True
    api_id = account.get("api_id") or account.get("_raw", {}).get("id")
    if not api_id:
        raise ValueError(f"No numeric API id found for account {account.get('account_id')}")
    try:
        return client.decommission_account(event_slug, int(api_id), remain_allocated=remain)
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 500:
            # 500 may mean the action succeeded but the server crashed post-commit.
            # Verify by re-fetching the account status.
            try:
                acc_resp = client._get(f"/event_management/events/{event_slug}/accounts/{api_id}")
                status = acc_resp.get("status", "").lower() if isinstance(acc_resp, dict) else ""
                if "decommission" in status:
                    return {
                        "message": "Decommission likely succeeded (server returned 500 but account status is now decommissioned)",
                        "message_code": "decommission_500_but_verified",
                        "account_id": int(api_id),
                        "account_name": account.get("account_id", ""),
                        "status": status,
                        "deallocated": not remain,
                    }
            except Exception:
                pass  # Verification failed, fall through to original error
        # Surface the response body for better error messages
        body = ""
        if e.response is not None:
            try:
                body = e.response.json()
            except Exception:
                body = e.response.text[:500]
        raise RuntimeError(f"Decommission API error ({e.response.status_code}): {body}") from e


SERVICES = {
    "password_reset": {
        "service_type": "action",
        "label": "Reset HOL password",
        "description": "Resets the target user's password and applies a permissive password policy to allow the simple password.",
        "icon": ":material/lock_reset:",
        "get_statements": get_password_reset_statements,
        "get_preview": get_password_reset_preview,
        "render_config": render_password_reset_config,
    },
    "remove_mfa_method": {
        "service_type": "dynamic_action",
        "label": "Remove MFA methods",
        "description": "Removes all registered MFA methods from the target user by first listing them via SHOW MFA METHODS, then removing each one.",
        "icon": ":material/no_encryption:",
        "get_execute": get_remove_mfa_method_execute,
        "get_preview": get_remove_mfa_method_preview,
    },
    "mfa_disable": {
        "service_type": "action",
        "label": "Disable MFA temporarily",
        "description": "Bypasses MFA for the target user for a configurable number of minutes.",
        "icon": ":material/phonelink_lock:",
        "get_statements": get_mfa_bypass_statements,
        "get_preview": get_mfa_bypass_preview,
        "render_config": render_mfa_config,
    },
    # "consumption_metrics": {
    #     "service_type": "metrics",
    #     "label": "View consumption metrics",
    #     "description": "Fetches credit usage, warehouse breakdown, Cortex AI usage, query activity, and storage from ACCOUNT_USAGE (last 30 days). Note: ACCOUNT_USAGE has a ~45 min data lag.",
    #     "icon": ":material/bar_chart:",
    #     "get_queries": get_consumption_queries,
    #     "render_results": render_consumption_results,
    # },
    "account_locator": {
        "service_type": "output",
        "label": "Get account locator",
        "description": "Retrieves the Snowflake account locator (CURRENT_ACCOUNT()) for each selected account.",
        "icon": ":material/pin:",
        "get_statements": lambda: ["SELECT CURRENT_ACCOUNT() AS ACCOUNT_LOCATOR"],
        "get_preview": lambda: "SELECT CURRENT_ACCOUNT() AS ACCOUNT_LOCATOR",
        "output_column": "ACCOUNT_LOCATOR",
    },
    "disable_mfa_ddl": {
        "service_type": "output",
        "label": "Part 1: Generate ALTER ACCOUNT DDL",
        "description": "Fetches account locators and generates ALTER ACCOUNT DDL to disable REQUIRE_SNOWSIGHT_MFA. Copy output to run in the production admin account.",
        "icon": ":material/content_copy:",
        "render_config": render_disable_mfa_ddl_config,
        "get_statements": lambda: ["SELECT CURRENT_ACCOUNT() AS ACCOUNT_LOCATOR"],
        "get_preview": get_disable_mfa_ddl_preview,
        "output_column": "ACCOUNT_LOCATOR",
        "render_output": generate_disable_mfa_ddl,
        "group": "disable_account_mfa",
    },
    "disable_mfa_policy": {
        "service_type": "action",
        "label": "Part 2: Apply auth policy on accounts",
        "description": "Creates and applies an authentication policy with MFA_ENROLLMENT='OPTIONAL' on each selected account.",
        "icon": ":material/policy:",
        "get_statements": get_disable_mfa_policy_statements,
        "get_preview": get_disable_mfa_policy_preview,
        "group": "disable_account_mfa",
    },
    "custom_sql": {
        "service_type": "custom_sql",
        "label": "Run custom SQL",
        "description": "Execute arbitrary SQL statements on each selected account. Statements are separated by semicolons and run sequentially.",
        "icon": ":material/terminal:",
        "get_statements": get_custom_sql_statements,
        "get_preview": get_custom_sql_preview,
        "render_config": render_custom_sql_config,
    },
    "decommission": {
        "service_type": "api_action",
        "label": "Decommission account",
        "description": "Decommissions the account via the DataOps API. This resets the account to a clean state for reuse.",
        "icon": ":material/delete_sweep:",
        "render_config": render_decommission_config,
        "get_preview": get_decommission_preview,
        "execute": execute_decommission,
    },
}

SERVICE_GROUPS = {
    "disable_account_mfa": {
        "label": "Disable account MFA",
        "icon": ":material/shield_lock:",
        "description": "Two-part process to disable MFA enforcement on accounts.",
    },
}

def resolve_service_configs(selected_services: List[str]) -> Dict[str, Dict]:
    """Pre-resolve all service configs on the main Streamlit thread.
    Must be called before spawning background threads — session_state is not
    accessible from worker threads."""
    configs = {}
    for svc_key in selected_services:
        svc = SERVICES[svc_key]
        cfg: Dict = {"service_type": svc.get("service_type", "action")}
        if "get_statements" in svc:
            cfg["statements"] = svc["get_statements"]()
        if "get_queries" in svc:
            cfg["queries"] = svc["get_queries"]()
        if "render_results" in svc:
            cfg["render_results"] = svc["render_results"]
        if cfg["service_type"] == "api_action":
            cfg["execute"] = svc["execute"]
            cfg["remain_allocated"] = st.session_state.get("decommission_remain_allocated", True)
        if cfg["service_type"] == "dynamic_action":
            cfg["execute"] = svc["get_execute"]()
        if "output_column" in svc:
            cfg["output_column"] = svc["output_column"]
        configs[svc_key] = cfg
    return configs


def _run_services_core(account: Dict, conn_params: dict, service_configs: Dict[str, Dict], api_client=None, event_slug: Optional[str] = None) -> Dict:
    """Thread-safe core: no session_state access. All configs passed as plain data."""
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

    # --- Handle API-action services first (no Snowflake connection needed) ---
    api_configs = {k: v for k, v in service_configs.items() if v.get("service_type") == "api_action"}
    sql_configs = {k: v for k, v in service_configs.items() if v.get("service_type") != "api_action"}

    for svc_key, cfg in api_configs.items():
        svc_result = {"success": False, "error": None}
        try:
            if not api_client:
                raise ValueError("DataOps API client not available for api_action service")
            if not event_slug:
                raise ValueError("Event slug not available for api_action service")
            resp = cfg["execute"](api_client, event_slug, account, cfg)
            svc_result["success"] = True
            svc_result["data"] = resp
        except Exception as e:
            svc_result["error"] = str(e)
            all_ok = False
        result["services"][svc_key] = svc_result

    # --- Handle SQL-based services (need Snowflake connection) ---
    if sql_configs:
        try:
            conn = snowflake.connector.connect(
                account=account["conn_account"],
                warehouse="COMPUTE_WH",
                **conn_params,
            )
            cursor = conn.cursor()

            for svc_key, cfg in sql_configs.items():
                svc_result = {"success": False, "error": None}
                try:
                    if cfg["service_type"] == "metrics":
                        panels = []
                        for q in cfg["queries"]:
                            cursor.execute(q["sql"])
                            rows = cursor.fetchall()
                            columns = [d[0] for d in cursor.description] if cursor.description else []
                            panels.append({"label": q["label"], "columns": columns, "rows": rows})
                        svc_result["data"] = panels
                        svc_result["success"] = True
                    elif cfg["service_type"] in ("custom_sql", "output"):
                        stmt_results = []
                        for stmt in cfg["statements"]:
                            sr = {"sql": stmt, "success": False, "error": None, "columns": None, "rows": None}
                            try:
                                cursor.execute(stmt)
                                desc = cursor.description
                                rows = cursor.fetchall()
                                if desc and rows:
                                    sr["columns"] = [col[0] for col in desc]
                                    sr["rows"] = [list(r) for r in rows[:50]]
                                sr["success"] = True
                            except Exception as stmt_e:
                                sr["error"] = str(stmt_e)
                            stmt_results.append(sr)
                        svc_result["statement_results"] = stmt_results
                        if all(s["success"] for s in stmt_results):
                            svc_result["success"] = True
                        else:
                            all_ok = False
                    elif cfg["service_type"] == "dynamic_action":
                        cfg["execute"](cursor)
                        svc_result["success"] = True
                    else:
                        for stmt in cfg["statements"]:
                            cursor.execute(stmt)
                        svc_result["success"] = True
                except Exception as e:
                    svc_result["error"] = str(e)
                    all_ok = False
                result["services"][svc_key] = svc_result

            cursor.close()
            conn.close()

        except Exception as e:
            logger.error("Connection failed for %s: %s", account["conn_account"], e)
            result["error"] = str(e)
            for svc_key in sql_configs:
                result["services"][svc_key] = {"success": False, "error": None}
            all_ok = False

    result["success"] = all_ok
    return result


def run_services_on_account(account: Dict, selected_services: List[str]) -> Dict:
    """Thin wrapper that resolves credentials + configs from session state then delegates to core."""
    conn_params = resolve_connection_params()
    service_configs = resolve_service_configs(selected_services)
    return _run_services_core(account, conn_params, service_configs, api_client=None, event_slug=None)


def _circle_for_result(result: Dict) -> str:
    if result["success"]:
        has_warning = any(not s.get("success") for s in result.get("services", {}).values())
        return "🟡" if has_warning else "🟢"
    return "🔴"


def _run_apply_job(
    accounts: List[Dict],
    service_configs: Dict[str, Dict],
    conn_params: dict,
    parallel: bool,
    max_workers: int,
    api_client=None,
    event_slug: Optional[str] = None,
):
    """Background thread: runs services on all accounts, writes progress to _apply_job."""
    order = [acc["account_id"] for acc in accounts]
    total = len(accounts)
    done_map: Dict = {}

    def _record(result):
        done_map[result["account_id"]] = result
        circles = [
            _circle_for_result(done_map[aid]) if aid in done_map else "⚪"
            for aid in order
        ]
        with _apply_lock:
            _apply_job["results"].append(result)
            _apply_job["completed"] = len(done_map)
            _apply_job["circles"] = circles

    logger.info("Apply job started: %d accounts, parallel=%s, max_workers=%d", total, parallel, max_workers)
    try:
        if parallel:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_acc = {
                    executor.submit(
                        _run_services_core, acc, conn_params, service_configs, api_client, event_slug
                    ): acc
                    for acc in accounts
                }
                for future in as_completed(future_to_acc):
                    if _apply_job["cancel"].is_set():
                        for f in future_to_acc:
                            f.cancel()
                        break
                    result = future.result()
                    _record(result)
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
                result = _run_services_core(acc, conn_params, service_configs, api_client, event_slug)
                _record(result)
    finally:
        with _apply_lock:
            _apply_job["running"] = False
            _apply_job["cancelled"] = _apply_job["cancel"].is_set()
            _apply_job["status"] = "Cancelled." if _apply_job["cancelled"] else "Done!"
        logger.info("Apply job finished: completed=%d/%d, cancelled=%s", len(done_map), total, _apply_job["cancelled"])


# =============================================================================
# Sidebar — DataOps API Configuration
# =============================================================================

# Resolve API token: secrets.toml > env var > manual input
_token_from_secrets = st.secrets.get("GITLAB_API_TOKEN", "") or os.environ.get("DATAOPS_API_TOKEN", "") or os.environ.get("GITLAB_API_TOKEN", "")
_token_is_preconfigured = bool(_token_from_secrets)

with st.sidebar:
    st.header(":material/key: DataOps API")

    if _token_is_preconfigured:
        # Token is configured via secrets/env — don't show the input field
        api_token = _token_from_secrets
        st.success("Token configured via secrets", icon=":material/check_circle:")
    else:
        # No pre-configured token — show input field
        api_token = st.text_input(
            "GitLab API Token",
            value="",
            type="password",
            help="GitLab API token from https://app.dataops.live/-/profile/personal_access_tokens. Can also be set in .streamlit/secrets.toml as GITLAB_API_TOKEN.",
            placeholder="glpat-xxxxxxxxxxxxxxxxxxxx",
        )

    if api_token:
        client = DataOpsClient(api_token)
        try:
            client.health_check()
            st.session_state.dataops_connected = True
            st.session_state.dataops_auth_method = client.auth_method
            st.success(f"Connected ({client.auth_method.upper()})", icon=":material/check_circle:")
        except requests.exceptions.HTTPError as e:
            st.session_state.dataops_connected = False
            st.session_state.dataops_auth_method = None
            st.error(f"Auth failed: {e.response.status_code}", icon=":material/error:")
            client = None
        except Exception as e:
            st.session_state.dataops_connected = False
            st.session_state.dataops_auth_method = None
            st.error(f"Connection error: {e}", icon=":material/error:")
            client = None
    else:
        client = None
        st.session_state.dataops_connected = False

    st.divider()
    st.caption("Token is tried as PAT (`private-token` header) first, then as Bearer token.")

# =============================================================================
# Main UI
# =============================================================================

st.title("DataOps.live Hands-On Lab Commander")
st.caption("Apply administrative operations across Snowflake hands-on lab accounts")

# --- GitLab API token unconfigured warning panel ---
if not _token_is_preconfigured and not api_token:
    with st.container(border=True):
        st.warning(
            ":material/vpn_key: **GitLab API token not configured** — Enter your token in the sidebar, or set `GITLAB_API_TOKEN` in `.streamlit/secrets.toml`.",
            icon=":material/warning:",
        )

# =============================================================================
# Section 1: Event Selection (API-driven)
# =============================================================================

st.subheader(":material/event: Event selection")

if client and st.session_state.dataops_connected:
    # --- Quick Access ---
    pinned = get_pinned_events()
    st.caption("**Quick access**")
    pin_cols = st.columns(min(len(pinned) + 1, 6))
    for i, event in enumerate(pinned):
        col = pin_cols[i % len(pin_cols)]
        with col:
            is_hardcoded = event["slug"] in [e["slug"] for e in HARDCODED_EVENTS]
            btn_label = f":material/bolt: {event['name']}" if is_hardcoded else f":material/star: {event['name']}"
            if st.button(btn_label, key=f"pin_{event['slug']}", use_container_width=True):
                st.session_state.selected_event_slug = event["slug"]
                st.session_state.api_accounts = []
                st.session_state.api_accounts_raw = []
                st.session_state.account_source_events = {}
                st.session_state.pop("event_filter_state", None)
                st.rerun()
            if not is_hardcoded:
                if st.button(":material/close:", key=f"unpin_{event['slug']}", help="Remove from quick access"):
                    remove_favorite(event["slug"])
                    st.rerun()

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
                st.error(f"Search failed: {e}", icon=":material/error:")
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
        with st.expander(f"Search results ({len(active_results)})", expanded=show_results, icon=":material/list:"):
            for idx, evt in enumerate(active_results):
                evt_name = evt.get("name", "Unknown")
                evt_slug = evt.get("slug", "")
                evt_location = evt.get("location", "")
                display = f"**{evt_name}** ({evt_slug})"
                if evt_location:
                    display += f" — {evt_location}"

                c1, c2, c3, c4 = st.columns([6, 1, 1, 1])
                with c1:
                    st.markdown(display)
                with c2:
                    if st.button(":material/check:", key=f"select_evt_{idx}", help="Select this event"):
                        st.session_state.selected_event_slug = evt_slug
                        st.session_state.api_accounts = []
                        st.session_state.api_accounts_raw = []
                        st.session_state.account_source_events = {}
                        st.session_state.pop("event_filter_state", None)
                        st.rerun()
                with c3:
                    if st.button(":material/add:", key=f"merge_evt_{idx}", help="Add accounts from this event to current pool"):
                        st.session_state._merge_event_slug = evt_slug
                        st.session_state._merge_event_name = evt_name
                        st.rerun()
                with c4:
                    if st.button(":material/star:", key=f"fav_evt_{idx}", help="Pin to quick access"):
                        add_favorite(evt_slug, evt_name)
                        st.rerun()

        with st.expander(":material/bug_report: Debug: Raw event search results", expanded=False):
            st.json(st.session_state.event_search_results[:5])

    # --- Merge accounts from another event into current pool ---
    if st.session_state.get("_merge_event_slug"):
        merge_slug = st.session_state._merge_event_slug
        merge_name = st.session_state.get("_merge_event_name", merge_slug)
        try:
            with st.spinner(f"Fetching accounts from **{merge_name}**..."):
                merge_raw = client.get_all_event_accounts(merge_slug)
            merge_accounts = [api_account_to_internal(a) for a in merge_raw]
            # Deduplicate by account_id — keep existing entries, add new ones
            existing_ids = {a["account_id"] for a in st.session_state.api_accounts}
            new_accounts = [a for a in merge_accounts if a["account_id"] not in existing_ids]
            new_raw = [r for r in merge_raw if api_account_to_internal(r)["account_id"] not in existing_ids]
            st.session_state.api_accounts.extend(new_accounts)
            st.session_state.api_accounts_raw.extend(new_raw)
            # Tag merged accounts with their source event
            for acc in new_accounts:
                st.session_state.account_source_events[acc["account_id"]] = merge_slug
            if new_accounts:
                st.success(
                    f"Added **{len(new_accounts)}** account(s) from **{merge_name}** "
                    f"({len(merge_accounts) - len(new_accounts)} duplicate(s) skipped)",
                    icon=":material/playlist_add:",
                )
            else:
                st.info(
                    f"All **{len(merge_accounts)}** account(s) from **{merge_name}** were already in the pool",
                    icon=":material/info:",
                )
        except Exception as e:
            st.error(f"Failed to fetch accounts from {merge_name}: {e}", icon=":material/error:")
        finally:
            st.session_state._merge_event_slug = None
            st.session_state._merge_event_name = None

    # --- Load accounts for selected event ---
    if st.session_state.selected_event_slug:
        st.divider()
        st.markdown(f"### :material/event: `{st.session_state.selected_event_slug}`")

        if not st.session_state.api_accounts:
            try:
                with st.spinner("Fetching all accounts (paginating)..."):
                    raw_accounts = client.get_all_event_accounts(st.session_state.selected_event_slug)
                st.session_state.api_accounts_raw = raw_accounts
                st.session_state.api_accounts = [api_account_to_internal(a) for a in raw_accounts]
                # Tag accounts with their source event
                for acc in st.session_state.api_accounts:
                    st.session_state.account_source_events[acc["account_id"]] = st.session_state.selected_event_slug
            except Exception as e:
                st.error(f"Failed to load accounts: {e}", icon=":material/error:")

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
                st.caption(f":material/filter_alt: Hiding **{filtered_count}** decommissioned account(s)")

        st.session_state["_visible_api_accounts"] = visible_api_accounts

        if st.session_state.api_accounts:
            st.success(
                f"Loaded **{len(st.session_state.api_accounts)}** total account(s) from API "
                f"(**{len(visible_api_accounts)}** active)",
                icon=":material/cloud_done:",
            )
        elif st.session_state.api_accounts_raw is not None:
            st.warning("No accounts found for this event", icon=":material/warning:")

        with st.expander(":material/bug_report: Debug: Raw API response", expanded=False):
            if st.session_state.api_accounts_raw:
                st.json(st.session_state.api_accounts_raw[:3])
                if len(st.session_state.api_accounts_raw) > 3:
                    st.caption(f"... and {len(st.session_state.api_accounts_raw) - 3} more")
            else:
                st.caption("No raw data available")

        if st.button(":material/refresh: Reload accounts", key="reload_accounts"):
            st.session_state.api_accounts = []
            st.session_state.api_accounts_raw = []
            st.session_state.account_source_events = {}
            st.session_state.pop("event_filter_state", None)
            st.rerun()

        st.divider()
        st.caption("**Event actions**")
        if st.button(":material/replay: Re-run configure pipeline", key="rerun_pipeline_btn", use_container_width=True):
            st.session_state._confirm_rerun_pipeline = True

        if st.session_state.get("_confirm_rerun_pipeline"):
            st.warning(
                f"This will re-run the configure pipeline for **{st.session_state.selected_event_slug}**. "
                "This affects all accounts in the event pool.",
                icon=":material/warning:",
            )
            c1, c2 = st.columns(2)
            with c1:
                if st.button(":material/check: Confirm", key="confirm_rerun", type="primary", use_container_width=True):
                    st.session_state._confirm_rerun_pipeline = False
                    try:
                        client.rerun_configure_pipeline(st.session_state.selected_event_slug)
                        st.success("Pipeline triggered successfully!", icon=":material/check_circle:")
                    except Exception as e:
                        st.error(f"Failed: {e}", icon=":material/error:")
            with c2:
                if st.button(":material/close: Cancel", key="cancel_rerun", use_container_width=True):
                    st.session_state._confirm_rerun_pipeline = False
                    st.rerun()

        if st.button(":material/clear: Clear event selection", key="clear_event"):
            st.session_state.selected_event_slug = None
            st.session_state.api_accounts = []
            st.session_state.api_accounts_raw = []
            st.session_state.event_search_results = []
            st.session_state.account_source_events = {}
            st.rerun()

else:
    st.info("Configure your DataOps API token in the sidebar to enable event search.", icon=":material/info:")

# =============================================================================
# Section 1b: CSV Fallback
# =============================================================================

with st.expander(":material/upload_file: Manual CSV input (fallback)", expanded=not st.session_state.dataops_connected):
    csv_input = st.text_area(
        "Account CSV",
        height=150,
        placeholder="""Account ID,Status,Assigned To,URL
MAKE_YOUR_DATA_AI_READY_RETAIL_DBSFEV,ready,user@snowflake.com,https://...
MAKE_YOUR_DATA_AI_READY_RETAIL_XIGYMV,ready,user2@snowflake.com,https://...""",
        help="Paste CSV data from the Preparation tab of your event in Dataops.live"
    )
    csv_accounts = parse_account_csv(csv_input) if csv_input else []
    if csv_accounts:
        st.success(f"Parsed **{len(csv_accounts)}** account(s) from CSV", icon=":material/check_circle:")

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
# Section 2: Account selection (shared UI for both sources)
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
        # Enable parallelism by default when event has more than 1 active account
        if len(accounts) > 1:
            st.session_state.parallel_execution = True
            st.session_state.parallel_workers = 5

    source_label = "API" if account_source == "api" else "CSV"
    st.caption(f":material/info: **{len(accounts)}** account(s) loaded from {source_label}")

    search_col, clear_col = st.columns([11, 1], vertical_alignment="bottom")
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
                st.rerun()

    # --- Event source filter (shows all events in the pool) ---
    source_events = st.session_state.get("account_source_events", {})
    unique_events = sorted(set(source_events.values()))
    if unique_events:
        st.caption("**Events in pool**")
        with st.container(border=True):
            # Initialize filter state for each event
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

    with st.container(horizontal=True):
        if st.button("Select all visible", use_container_width=True):
            for acc in visible_accounts:
                st.session_state.selected_accounts.add(acc["account_id"])
                st.session_state[f"acc_{acc['account_id']}"] = True
            st.rerun()
        if st.button("Select none", use_container_width=True):
            st.session_state.selected_accounts = set()
            for acc in accounts:
                st.session_state[f"acc_{acc['account_id']}"] = False
            st.rerun()

    st.markdown(f":material/manage_accounts: **Select accounts** ({len(visible_accounts)})")
    # Build a lookup of results by account_id for status indicators
    _results_by_account = {}
    for r in st.session_state.results:
        _results_by_account[r["account_id"]] = r

    with st.container(border=True, height=400):
        if not visible_accounts:
            st.caption("No accounts match the search.")
        for acc in visible_accounts:
            is_selected = acc["account_id"] in st.session_state.selected_accounts

            # Determine status indicator
            status_icon = ""
            acc_result = _results_by_account.get(acc["account_id"])
            if acc_result is not None:
                if acc_result["success"]:
                    # Check for partial warnings (any service with an error)
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
            url_part = f" — [{acc['url']}]({acc['url']})" if acc.get("url") else ""
            display_name = f"{acc['suffix']}** — {assigned_display}{url_part}"
            label = f"{status_icon}**{display_name}"

            if st.checkbox(label, value=is_selected, key=f"acc_{acc['account_id']}"):
                st.session_state.selected_accounts.add(acc["account_id"])
            else:
                st.session_state.selected_accounts.discard(acc["account_id"])

    selected_count = len(st.session_state.selected_accounts)
    if selected_count > 0:
        st.caption(f":material/info: **{selected_count}** account(s) selected")
    else:
        st.warning("No accounts selected", icon=":material/warning:")

# =============================================================================
# Section 3: Admin services
# =============================================================================

st.subheader(":material/build: Admin services")
st.caption("Select the operations to apply to every selected account")

# --- Auth mode selector ---
if st.session_state.get("auth_mode") not in ("keypair", "password"):
    st.session_state["auth_mode"] = "keypair"

st.segmented_control(
    "Auth mode",
    options=["keypair", "password"],
    key="auth_mode",
    help="Key-pair uses EMERGENCY_SERVICE_USER with PEM key. Password uses ADMIN/USER with HOL password.",
)

if st.session_state.get("auth_mode") == "keypair":
    if _PRIVATE_KEY_PEM_SECRET:
        st.success("Key configured via secret (PRIVATE_KEY_PEM)", icon=":material/check_circle:")
    else:
        key_path = st.text_input(
            "PEM key path",
            value=DEFAULT_KEY_PATH,
            key="private_key_path",
            help="Path to the RSA private key PEM file for EMERGENCY_SERVICE_USER",
        )
    st.text_input(
        "Service user",
        value=DEFAULT_SERVICE_USER,
        key="service_user",
        help="Snowflake user to connect as (must have key-pair auth configured)",
    )
    # Validate key is loadable
    _key_check = _get_private_key_der()
    if _key_check:
        if not _PRIVATE_KEY_PEM_SECRET:
            st.success("Key loaded from file", icon=":material/check_circle:")
    else:
        st.error("PEM key not found or unreadable — falling back to password mode", icon=":material/error:")
        st.session_state["auth_mode"] = "password"
else:
    # Password mode — show existing target user / run-as controls
    if st.session_state.get("target_user") not in ("USER", "ADMIN", "Custom"):
        st.session_state["target_user"] = "USER"

    st.segmented_control(
        "Target user",
        options=["USER", "ADMIN", "Custom"],
        key="target_user",
        help="Which Snowflake user to execute the services against",
    )

    if st.session_state.get("run_as") not in ("ADMIN", "USER"):
        st.session_state["run_as"] = "ADMIN"

    st.segmented_control(
        "Run as",
        options=["ADMIN", "USER"],
        key="run_as",
        help="Which Snowflake user to connect as when executing commands",
    )

    if st.session_state.get("target_user") == "Custom":
        st.text_input(
            "Custom username",
            key="custom_target_user",
            placeholder="e.g. JOHN_DOE",
            help="Enter the Snowflake username to authenticate and run commands against",
        )
        st.text_input(
            "Custom password",
            key="custom_target_password",
            type="password",
            placeholder="Enter password",
            help="The password used to authenticate as the custom user",
        )

selected_services = []

with st.container(border=True):
    rendered_groups = set()
    for svc_key, svc in SERVICES.items():
        group_id = svc.get("group")

        # Render entire group in a sub-container the first time we see it
        if group_id and group_id not in rendered_groups:
            rendered_groups.add(group_id)
            grp = SERVICE_GROUPS.get(group_id, {})
            group_svcs = [(k, v) for k, v in SERVICES.items() if v.get("group") == group_id]
            with st.container(border=True):
                st.markdown(f"{grp.get('icon', '')} **{grp.get('label', group_id)}**")
                st.caption(grp.get("description", ""))
                for g_key, g_svc in group_svcs:
                    col1, col2 = st.columns([1, 8], vertical_alignment="top")
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
                            with st.expander("View SQL", icon=":material/code:"):
                                st.code(g_svc["get_preview"](), language="sql")
                    if g_checked:
                        selected_services.append(g_key)
                        st.session_state.active_services.add(g_key)
                    else:
                        st.session_state.active_services.discard(g_key)
            continue

        # Skip grouped services on subsequent encounters (already rendered above)
        if group_id:
            continue

        col1, col2 = st.columns([1, 8], vertical_alignment="top")
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
                with st.expander("View SQL", icon=":material/code:"):
                    st.code(svc["get_preview"](), language="sql")

        if checked:
            selected_services.append(svc_key)
            st.session_state.active_services.add(svc_key)
        else:
            st.session_state.active_services.discard(svc_key)

# =============================================================================
# Section 4: Apply
# =============================================================================

st.subheader(":material/play_circle: Apply")

selected_accounts_list = [acc for acc in accounts if acc["account_id"] in st.session_state.selected_accounts]
can_apply = bool(selected_accounts_list and selected_services)

if not accounts:
    st.caption("Select an event above or paste CSV data to get started.")
elif not selected_accounts_list:
    st.warning("Select at least one account to continue.", icon=":material/warning:")
elif not selected_services:
    st.warning("Select at least one service to apply.", icon=":material/warning:")
else:
    svc_labels = [SERVICES[k]["label"].replace("`", "") for k in selected_services]
    st.caption(
        f"Will run **{len(selected_services)}** service(s) on **{len(selected_accounts_list)}** account(s): "
        + ", ".join(svc_labels)
    )

only_metrics = all(SERVICES[k]["service_type"] == "metrics" for k in selected_services) if selected_services else False

# --- Parallel execution controls ---
par_col, workers_col = st.columns([3, 2], vertical_alignment="bottom")
with par_col:
    parallel_enabled = st.checkbox(
        ":material/bolt: Parallel execution",
        key="parallel_execution",
        help="Run accounts concurrently. Recommended for larger events (10+ accounts).",
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
    (
        f":material/bar_chart: Fetch metrics for {len(selected_accounts_list)} account(s)"
        if only_metrics
        else f":material/rocket_launch: Apply to {len(selected_accounts_list)} account(s)"
    )
    if selected_accounts_list
    else (":material/bar_chart: Fetch metrics" if only_metrics else ":material/rocket_launch: Apply"),
    type="primary",
    disabled=not can_apply or _apply_job["running"],
    use_container_width=True,
):
    conn_params = resolve_connection_params()
    parallel = st.session_state.get("parallel_execution", False)
    max_workers = st.session_state.get("parallel_workers", 5)
    # Resolve all service configs NOW on the main thread — session_state is
    # not accessible from background worker threads.
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
        args=(selected_accounts_list, service_configs, conn_params, parallel, max_workers, client, st.session_state.get("selected_event_slug")),
        daemon=True,
    ).start()
    st.rerun()

# --- Live progress + cancel (shown while job is running) ---
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
        ":material/stop: Cancel",
        key="cancel_apply_btn",
        type="secondary",
        use_container_width=True,
    ):
        _apply_job["cancel"].set()

    time.sleep(0.4)
    st.rerun()

elif st.session_state.get("_collecting_apply_results"):
    # Job just finished — collect results then clear the flag
    with _apply_lock:
        finished_results = list(_apply_job["results"])
    st.session_state.results = finished_results
    st.session_state._collecting_apply_results = False
    st.rerun()

# =============================================================================
# Section 5: Results
# =============================================================================

if st.session_state.results:
    st.subheader(":material/analytics: Results")

    success_count = sum(1 for r in st.session_state.results if r["success"])
    fail_count = len(st.session_state.results) - success_count

    col1, col2 = st.columns(2)
    with col1:
        st.metric("Successful", success_count)
    with col2:
        st.metric("Failed", fail_count)

    if st.session_state.get("results_filter") not in ("All", "Success", "Failure"):
        st.session_state["results_filter"] = "All"

    st.segmented_control(
        "Show",
        options=["All", "Success", "Failure"],
        key="results_filter",
    )

    results_filter = st.session_state.get("results_filter") or "All"
    if results_filter == "Success":
        filtered_results = [r for r in st.session_state.results if r["success"]]
    elif results_filter == "Failure":
        filtered_results = [r for r in st.session_state.results if not r["success"]]
    else:
        filtered_results = st.session_state.results

    render_consumption_dashboard(st.session_state.results)

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
                # Extract the output value from statement_results
                for sr in svc_result.get("statement_results", []):
                    if sr.get("success") and sr.get("columns") and sr.get("rows"):
                        if output_col and output_col in sr["columns"]:
                            col_idx = sr["columns"].index(output_col)
                            for row in sr["rows"]:
                                values.append(str(row[col_idx]))
                        else:
                            # Fallback: take first column value
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
                    if "render_output" in svc:
                        st.code(svc["render_output"](values), language="sql")
                    else:
                        st.code("\n".join(values), language=None)
                if failures:
                    with st.expander(f":material/error: {len(failures)} failed", expanded=False):
                        for f in failures:
                            st.caption(f"{f['suffix']}: {f['error']}")

    for result in filtered_results:
        if result["success"]:
            icon = ":material/check_circle:"
            status = "Success"
        else:
            icon = ":material/error:"
            status = "Failed"

        with st.expander(
            f"{icon} **{result['suffix']}** {result.get('assigned_to', '')}"
            + (f" — {result['url']}" if result.get("url") else "")
            + f" -- {status}",
            expanded=not result["success"],
            icon=icon,
        ):
            if result["error"]:
                st.error(f"Connection error: {result['error']}", icon=":material/error:")
            else:
                for svc_key, svc_result in result["services"].items():
                    svc = SERVICES[svc_key]
                    # Output services are rendered in the aggregate panel above
                    if svc.get("service_type") == "output":
                        if svc_result.get("success"):
                            st.markdown(f":green-badge[OK] {svc['icon']} {svc['label']}")
                        else:
                            st.markdown(f":red-badge[Failed] {svc['icon']} {svc['label']}")
                            if svc_result.get("error"):
                                st.caption(f"Error: {svc_result['error']}")
                        continue
                    if "render_results" in svc:
                        if svc_result["success"]:
                            render_consumption_results(svc_result.get("data", []))
                        else:
                            st.error(f"Failed to fetch metrics: {svc_result['error']}", icon=":material/error:")
                    elif svc.get("service_type") == "custom_sql":
                        stmt_results = svc_result.get("statement_results", [])
                        if stmt_results:
                            ok = sum(1 for s in stmt_results if s["success"])
                            fail = len(stmt_results) - ok
                            if fail == 0:
                                st.markdown(f":green-badge[OK] {svc['icon']} {svc['label']} ({ok}/{len(stmt_results)} passed)")
                            else:
                                st.markdown(f":red-badge[{fail} failed] {svc['icon']} {svc['label']} ({ok}/{len(stmt_results)} passed)")
                            for j, sr in enumerate(stmt_results):
                                icon_sr = ":material/check_circle:" if sr["success"] else ":material/error:"
                                preview = sr["sql"].replace('\n', ' ').strip()[:60]
                                with st.expander(f"{icon_sr} `{preview}`", expanded=not sr["success"]):
                                    st.code(sr["sql"], language="sql")
                                    if sr["error"]:
                                        st.error(sr["error"])
                                    if sr["success"] and sr.get("columns") and sr.get("rows"):
                                        df = pd.DataFrame(sr["rows"], columns=sr["columns"])
                                        st.dataframe(df, use_container_width=True, hide_index=True)
                                    elif sr["success"]:
                                        st.caption("Executed successfully (no result set)")
                        elif svc_result.get("error"):
                            st.error(f"Custom SQL error: {svc_result['error']}", icon=":material/error:")
                    else:
                        if svc_result["success"]:
                            st.markdown(f":green-badge[OK] {svc['icon']} {svc['label']}")
                        else:
                            st.markdown(f":red-badge[Failed] {svc['icon']} {svc['label']}")
                            if svc_result["error"]:
                                st.caption(f"Error: {svc_result['error']}")

# =============================================================================
# Footer
# =============================================================================

st.caption("DataOps.live Hands-On Lab Commander | Connects as ACCOUNTADMIN using configured credentials")
