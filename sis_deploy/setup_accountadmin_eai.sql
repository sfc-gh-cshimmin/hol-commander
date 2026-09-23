-- =============================================================================
-- HOL Commander v2 — ACCOUNTADMIN EAI setup (Step 3 of 3)
-- Run this AFTER setup_sysadmin.sql has created the secrets.
-- The EAI references the secrets so they must exist first.
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- 1. External Access Integration
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION HOL_COMMANDER_EAI
    ALLOWED_NETWORK_RULES          = (GRE_APPS.HOL_COMMANDER_APP.SNOWFLAKE_EGRESS_RULE)
    ALLOWED_AUTHENTICATION_SECRETS = (
        GRE_APPS.HOL_COMMANDER_APP.HOL_PRIVATE_KEY,
        GRE_APPS.HOL_COMMANDER_APP.DATAOPS_API_TOKEN,
        GRE_APPS.HOL_COMMANDER_APP.HOL_PASSWORD,        GRE_APPS.HOL_COMMANDER_APP.HOL_TEMP_PASSWORD
    )
    ENABLED = TRUE;

-- 2. Grant USAGE on the EAI to SYSADMIN (needed to reference it in sprocs/Streamlit)
GRANT USAGE ON INTEGRATION HOL_COMMANDER_EAI TO ROLE SYSADMIN;

-- =============================================================================
-- DONE. SYSADMIN can now run the remaining parts of setup_sysadmin.sql
-- (stored procedures and CREATE STREAMLIT) that reference the EAI.
-- =============================================================================
