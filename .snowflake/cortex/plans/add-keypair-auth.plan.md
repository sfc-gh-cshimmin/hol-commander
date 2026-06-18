# Plan: Add RSA Key-Pair Authentication to si-admin-v2

## Context
The event-manager repo authenticates to HOL Snowflake accounts using RSA key-pair authentication via `EMERGENCY_SERVICE_USER`. si-admin-v2 currently uses password auth (`ADMIN`/`USER` + `sn0wf@ll`). We want si-admin-v2 to default to key-pair auth, keeping password as a fallback.

## Design

### Auth modes
- **Key-pair (default):** Loads PEM from `~/SI_ACCOUNTS_PEM_KEY/priv_key.pem` (configurable). Connects as `EMERGENCY_SERVICE_USER` with `private_key=` param. Role defaults to `ACCOUNTADMIN`.
- **Password (fallback):** Current behavior — connects as `ADMIN`/`USER` with password from secrets.

### Changes

#### 1. `requirements.txt`
Add `cryptography>=41.0.0`

#### 2. New utility: `load_private_key()`
```python
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

DEFAULT_KEY_PATH = os.path.expanduser("~/SI_ACCOUNTS_PEM_KEY/priv_key.pem")
DEFAULT_SERVICE_USER = "EMERGENCY_SERVICE_USER"

def load_private_key(key_path: str) -> bytes:
    with open(key_path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(), password=None, backend=default_backend()
        )
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
```

#### 3. Refactor credentials resolution
Replace the current `resolve_connection_credentials() -> tuple[str, str]` pattern with a structure that carries both auth modes:

```python
def resolve_connection_params() -> dict:
    """Return kwargs suitable for snowflake.connector.connect() (minus account/warehouse)."""
    auth_mode = st.session_state.get("auth_mode", "keypair")
    
    if auth_mode == "keypair":
        key_path = st.session_state.get("private_key_path", DEFAULT_KEY_PATH)
        service_user = st.session_state.get("service_user", DEFAULT_SERVICE_USER)
        private_key_der = load_private_key(key_path)
        return {"user": service_user, "private_key": private_key_der, "role": "ACCOUNTADMIN"}
    else:
        # existing password logic
        user, password = _resolve_password_credentials()
        role = resolve_connection_role()
        return {"user": user, "password": password, **({"role": role} if role else {})}
```

#### 4. Update `_run_services_core` signature
Change from `(account, conn_user, conn_password, conn_role, ...)` to `(account, conn_params, ...)` where `conn_params` is the dict from above:

```python
conn = snowflake.connector.connect(
    account=account["conn_account"],
    warehouse="COMPUTE_WH",
    **conn_params,
)
```

#### 5. Sidebar UI additions
In the sidebar, add an "Auth Mode" section before the existing "Target user" controls:
- Segmented control: `Key-pair` | `Password`
- When key-pair: show the PEM path (text input, defaulting to `~/SI_ACCOUNTS_PEM_KEY/priv_key.pem`) and service user (defaults to `EMERGENCY_SERVICE_USER`)
- When password: show existing target user / run-as controls (current behavior)
- Status indicator showing if the key loaded successfully

#### 6. Update call chain
All callers of `_run_services_core` and `_run_apply_job` pass `conn_params` instead of separate `conn_user`/`conn_password`/`conn_role` args.

#### 7. `secrets.toml.example` additions
```toml
PRIVATE_KEY_PATH = "~/SI_ACCOUNTS_PEM_KEY/priv_key.pem"
SERVICE_USER = "EMERGENCY_SERVICE_USER"
```

## Backward compatibility
- If the PEM file doesn't exist at the configured path, the app shows a warning and disables key-pair mode (forcing fallback to password).
- All existing password-based functionality continues to work when "Password" mode is selected.
- The "Target user" and "Run as" controls only appear in password mode.
