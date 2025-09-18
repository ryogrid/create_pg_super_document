# pg_config

## Location
src/backend/utils/misc/pg_config.c: 24 - 50

## Overview
The pg_config function is a PostgreSQL system function that exposes the same configuration information as the external pg_config utility, but as a Set Returning Function (SRF) within the database.

## Definition


## Detailed Description
This function returns PostgreSQL installation configuration information as a set of rows, where each row contains a configuration parameter name and its corresponding value. It serves as the SQL interface equivalent to the command-line pg_config utility, allowing users to query build-time configuration settings directly from within the database.

The function uses the materialized SRF (Set Returning Function) framework to return multiple rows containing configuration data. It retrieves configuration information by calling get_configdata() with the current executable path and iterates through all configuration entries to build the result set.

The returned data includes various PostgreSQL build and installation parameters such as compile flags, library paths, version information, and other configuration details that were set during the PostgreSQL compilation and installation process.

## Parameters / Member Variables
- This function takes no explicit parameters (uses PG_FUNCTION_ARGS macro for standard PostgreSQL function interface)
- Uses fcinfo->resultinfo to access the ReturnSetInfo structure for SRF handling

## Dependencies
- Functions called/Symbols referenced:
  - InitMaterializedSRF
  - get_configdata
  - CStringGetTextDatum
  - tuplestore_putvalues
- Data structures used:
  - ReturnSetInfo
  - ConfigData
- Called from (representative examples):
  - SQL queries via the pg_config() function call
  - System catalog functions

## Notes and Other Information
- Located in src/backend/utils/misc/pg_config.c:24-50
- This function is typically exposed as a system function that can be called via SQL: SELECT * FROM pg_config();
- The function returns a two-column result set with 'name' and 'setting' columns
- It provides the same information as the external pg_config command-line utility but accessible from within PostgreSQL
- Uses the standard PostgreSQL SRF (Set Returning Function) pattern with materialized results
- The configuration data is obtained from the common/config_info module which contains build-time configuration information
- Returns Datum 0 as is standard for SRF functions (actual results are stored in the tuplestore)