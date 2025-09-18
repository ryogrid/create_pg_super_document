# check_default_tablespace

## Location
src/backend/commands/tablespace.c: 1091 - 1142

## Overview
Validates the default_tablespace GUC (Grand Unified Configuration) parameter by verifying that the specified tablespace exists in the system catalog.

## Definition
bool check_default_tablespace(char **newval, void **extra, GucSource source)

## Detailed Description
This function serves as a validation hook for PostgreSQL's default_tablespace configuration parameter. It is called whenever the default_tablespace setting is being modified to ensure the specified tablespace actually exists. The function performs different validation behaviors based on the context: it only performs catalog lookups when inside a valid transaction state and connected to a database. For test scenarios (PGC_S_TEST source), it issues a NOTICE rather than failing hard when a tablespace doesn't exist. The function handles empty string values as valid (representing no default tablespace). This validation ensures that the default_tablespace GUC always contains a valid reference or empty string.

## Parameters / Member Variables
- newval: Pointer to pointer containing the new tablespace name being validated
- extra: Pointer for storing additional data (unused in this function)
- source: GucSource indicating the context of the parameter change (configuration file, command line, test, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [IsTransactionState](../I/IsTransactionState.md): Checks if currently in a valid transaction
  - [get_tablespace_oid](../g/get_tablespace_oid.md): Retrieves OID of tablespace by name, with option to suppress errors
  - ereport: PostgreSQL error/notice reporting function
  - GUC_check_errdetail: Sets detailed error message for GUC validation failures
  - MyDatabaseId: Global variable containing current database OID
  - InvalidOid: Constant representing an invalid object identifier
  - PGC_S_TEST: GUC source constant for test configurations

- Called from (representative examples):
  - Referenced in GUC_HOOKS_H: Declared as a GUC validation hook function

## Notes and Other Information
- Part of PostgreSQL's GUC (Grand Unified Configuration) validation framework
- Only performs validation when in a transaction state and connected to a database
- Handles test scenarios gracefully by issuing notices instead of errors
- Empty string values are considered valid, representing no default tablespace
- Returns false to reject invalid tablespace names, true to accept
- Critical for maintaining referential integrity of the default_tablespace setting
- Prevents runtime errors that could occur if default_tablespace referenced a non-existent tablespace
- Used during configuration loading, SET commands, and configuration validation