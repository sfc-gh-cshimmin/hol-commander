# Plan: Add "output" Service Type

## Problem
The "Get account locator" service uses `custom_sql` type, which renders results per-account in individual expanders. This makes it hard to copy all locators at once. More broadly, some services produce **output to consume** (locators, warehouse lists, user lists) vs. **mutations** (password reset, MFA disable). These need different result presentation.

## Design

### New `service_type: "output"`

A service that runs SQL and produces a single scalar (or row) of output per account. The key difference from other types:

- **Execution**: Same as `custom_sql` — runs statements, captures results
- **Results rendering**: Instead of showing inside each account's expander, results from all accounts are **aggregated** into a single panel at the top of the Results section with a copyable code block

### Service Definition

```python
"account_locator": {
    "service_type": "output",
    "label": "Get account locator",
    "description": "Retrieves the Snowflake account locator for each selected account.",
    "icon": ":material/pin:",
    "get_statements": lambda: ["SELECT CURRENT_ACCOUNT() AS ACCOUNT_LOCATOR"],
    "get_preview": lambda: "SELECT CURRENT_ACCOUNT() AS ACCOUNT_LOCATOR",
    "output_column": "ACCOUNT_LOCATOR",  # which column to extract as the output value
},
```

### Execution in `_run_services_core`

The `"output"` type behaves like `"custom_sql"` internally — executes statements, captures columns/rows in `statement_results`. No change needed to the execution engine; we just need the rendering logic to handle it differently.

Actually — simplest approach: keep execution identical to `custom_sql` (reuse that branch). The only difference is in **rendering**.

### Results Rendering

In the Results section (Section 5), **before** the per-account expanders:

1. Check if any selected services have `service_type == "output"`
2. For each output service, collect all successful results across accounts
3. Render an aggregated panel:

```
┌─────────────────────────────────────────────┐
│ 📌 Get account locator (47/50 succeeded)    │
│ ┌─────────────────────────────────────────┐ │
│ │ ILXVDC                                  │ │
│ │ ABCDEF                                  │ │
│ │ GHIJKL                                  │ │
│ │ ...                                     │ │
│ └─────────────────────────────────────────┘ │
│ 3 failed: [expander with errors]            │
└─────────────────────────────────────────────┘
```

The values are shown in `st.code()` (one per line, easily copyable). Failed accounts are shown in a nested expander below.

### Per-account expanders

Services with `service_type == "output"` are **skipped** in the per-account expander rendering loop. They've already been shown in the aggregate panel.

## Changes

| File | What |
|------|------|
| `app.py` line ~775 | Change `account_locator` service to `service_type: "output"`, add `output_column` key |
| `app.py` lines ~883 | Add `"output"` to the `elif` chain in `_run_services_core` (or just let it fall through to the same `custom_sql` handling) |
| `app.py` lines ~1651 | Add aggregate output panel rendering before per-account expanders |
| `app.py` lines ~1698 | Skip `output`-type services in per-account expander loop |

## Alternative Considered

- Making `output` a completely separate execution path with a dedicated `get_output()` function returning just a scalar. Rejected because reusing the `custom_sql` execution gives us error handling, multi-statement support, and column/row capture for free.
