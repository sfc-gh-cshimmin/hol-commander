# Plan: Decommission Service

## Context

The DataOps Admin API has a decommission endpoint:
```
POST /api/v1/event_management/events/{event_slug}/accounts/{account_id}/decommission
```

**Parameters:**
- `event_slug` (path, string, required) — the event slug
- `account_id` (path, integer, required) — the account's numeric DB `id` (not the identifier/slug string)
- `remain_allocated` (query, boolean, default: true) — whether the account stays allocated after decommission

**Response (200):**
```json
{
  "message": "string",
  "message_code": "string", 
  "account_id": integer,
  "account_name": "string",
  "status": "string",
  "deallocated": boolean
}
```

## Design Decisions

1. **New service_type: `api_action`** — The existing services are all SQL-based (they connect to Snowflake and run queries). Decommission is fundamentally different: it calls the DataOps REST API. I'll add a new `service_type="api_action"` with an `execute` function that receives the client, event slug, and account data.

2. **Account ID mapping** — The decommission endpoint needs the integer `id` from `ChildAccountSchema`, not the string `identifier`. The raw response is already stored in `_raw`, so we can access it as `account["_raw"]["id"]`. For clarity, I'll also store it as `api_id` in the internal dict.

3. **Execution flow** — Unlike SQL services, API actions don't need Snowflake credentials. The execution path in `_run_services_core` will check for `api_action` type and call the service's `execute` function directly.

4. **Config UI** — A simple checkbox for `remain_allocated` (default: True).

## Implementation Steps

### 1. Add `decommission_account()` to `DataOpsClient`

```python
def decommission_account(self, event_slug: str, account_id: int, remain_allocated: bool = True):
    params = f"?remain_allocated={'true' if remain_allocated else 'false'}"
    return self._post(
        f"/event_management/events/{event_slug}/accounts/{account_id}/decommission{params}"
    )
```

Note: The `_post` method doesn't support query params natively, so we'll either append to the URL or add a `params` kwarg to `_post`.

### 2. Update `api_account_to_internal()` 

Add `"api_id": api_acc.get("id")` to the returned dict so the numeric ID is easily accessible.

### 3. Create service helpers

```python
def render_decommission_config():
    st.session_state.setdefault("decommission_remain_allocated", True)
    st.checkbox(
        "Keep account allocated after decommission",
        key="decommission_remain_allocated",
        help="If checked, the account remains allocated to the user but is decommissioned. If unchecked, the account is also deallocated."
    )

def get_decommission_preview():
    remain = st.session_state.get("decommission_remain_allocated", True)
    return f"POST .../decommission?remain_allocated={remain}"
```

### 4. Register in SERVICES dict

```python
"decommission": {
    "service_type": "api_action",
    "label": "Decommission account",
    "description": "Decommissions the account via the DataOps API. This resets the account to a clean state.",
    "icon": ":material/delete_sweep:",
    "render_config": render_decommission_config,
    "get_preview": get_decommission_preview,
    "execute": execute_decommission,
},
```

### 5. Add execution function and wire into `_run_services_core`

```python
def execute_decommission(client: DataOpsClient, event_slug: str, account: Dict, config: Dict) -> Dict:
    remain = config.get("remain_allocated", True)
    api_id = account.get("api_id") or account["_raw"]["id"]
    return client.decommission_account(event_slug, api_id, remain_allocated=remain)
```

In `_run_services_core`, add a branch before the Snowflake connection:
- Separate API-action services from SQL services
- Execute API actions using the passed-in client and event_slug
- Only open a Snowflake connection if SQL services remain

### 6. Thread the client and event_slug through the execution pipeline

`resolve_service_configs` and `_run_apply_job` will need the `client` and `event_slug` passed through (or added to service_configs for api_action types).
