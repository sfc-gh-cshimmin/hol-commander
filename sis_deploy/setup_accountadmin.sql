-- =============================================================================
-- HOL Commander v2 — ACCOUNTADMIN-only setup (Step 1 of 3)
-- Run this FIRST to create the database, schema, network rule, and grant
-- ownership to SYSADMIN.
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- 1. Database and Schema
CREATE DATABASE IF NOT EXISTS GRE_APPS;
CREATE SCHEMA IF NOT EXISTS GRE_APPS.HOL_COMMANDER_APP;

-- 2. Network Rule
CREATE OR REPLACE NETWORK RULE GRE_APPS.HOL_COMMANDER_APP.SNOWFLAKE_EGRESS_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = (
        '*.snowflakecomputing.com:443',
        'admin.dataops.live:443'
    );

-- 3. Grant ownership to SYSADMIN
GRANT OWNERSHIP ON DATABASE GRE_APPS TO ROLE SYSADMIN COPY CURRENT GRANTS;
GRANT OWNERSHIP ON SCHEMA GRE_APPS.HOL_COMMANDER_APP TO ROLE SYSADMIN COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL NETWORK RULES IN SCHEMA GRE_APPS.HOL_COMMANDER_APP TO ROLE SYSADMIN COPY CURRENT GRANTS;

-- =============================================================================
-- DONE. Now have SYSADMIN run setup_sysadmin.sql (Step 2).
-- Then come back and run setup_accountadmin_eai.sql (Step 3).
-- =============================================================================
