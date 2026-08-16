-- Ergasterion one-time Snowflake account setup.
--
-- Run this file with an account administrator before using the live Snowflake demo:
--
--   snow sql -c dpf -f snowflake/setup.sql
--
-- The script is idempotent. It creates a dedicated database, an extra-small warehouse,
-- and a least-privilege build role. It does not create users, store credentials, configure
-- repository access, or enable external network access. Dependencies are downloaded locally
-- before the dbt project is deployed.

USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS ERGASTERION
  COMMENT = 'Ergasterion example data products';

CREATE WAREHOUSE IF NOT EXISTS DPF_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Ergasterion build warehouse';

CREATE ROLE IF NOT EXISTS DPF_BUILDER
  COMMENT = 'Least-privilege role for Ergasterion dbt builds';

GRANT USAGE ON DATABASE ERGASTERION TO ROLE DPF_BUILDER;
GRANT CREATE SCHEMA ON DATABASE ERGASTERION TO ROLE DPF_BUILDER;
GRANT USAGE ON SCHEMA ERGASTERION.PUBLIC TO ROLE DPF_BUILDER;
GRANT CREATE DBT PROJECT ON SCHEMA ERGASTERION.PUBLIC TO ROLE DPF_BUILDER;
GRANT CREATE STREAMLIT ON SCHEMA ERGASTERION.PUBLIC TO ROLE DPF_BUILDER;
GRANT USAGE ON WAREHOUSE DPF_WH TO ROLE DPF_BUILDER;

-- Grant DPF_BUILDER to the Snowflake user used by your CLI connection. Replace the
-- placeholder before running this statement:
--
--   GRANT ROLE DPF_BUILDER TO USER YOUR_SNOWFLAKE_USER;

SHOW WAREHOUSES LIKE 'DPF_WH';
SHOW ROLES LIKE 'DPF_BUILDER';
