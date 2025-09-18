# check_default_table_access_method

## Location
[src/backend/access/table/tableamapi.c:105-152](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/table/tableamapi.c#L105-L152)

## Overview
check_default_table_access_method is a PostgreSQL GUC (Grand Unified Configuration) check hook function that validates new values for the default_table_access_method configuration parameter.

## Definition
```c
bool check_default_table_access_method(char **newval, void **extra, GucSource source)
```

## Detailed Description
This function serves as a validation hook for the default_table_access_method GUC parameter, ensuring that any new value assigned to this configuration setting is valid. The function performs multiple validation checks including ensuring the value is not empty, does not exceed the maximum allowed length (NAMEDATALEN), and when possible, verifies that the specified table access method actually exists in the system catalogs.

The function implements context-aware validation - when running outside of a transaction or when not connected to a database, it cannot perform catalog lookups and must accept the value on faith. For test scenarios (PGC_S_TEST source), it issues only a NOTICE for nonexistent access methods rather than a hard error, allowing configuration testing without breaking the system.

## Parameters / Member Variables
- `newval`: Pointer to the new string value being validated for the default_table_access_method parameter
- `extra`: Pointer for storing additional data (unused in this function)
- `source`: GucSource indicating where the configuration change is coming from (e.g., configuration file, SET command, test scenario)

## Dependencies
- Functions called/Symbols referenced:
  - GUC_check_errdetail
  - [IsTransactionState](../I/IsTransactionState.md)
  - [get_table_am_oid](../g/get_table_am_oid.md)
  - ereport
  - NAMEDATALEN (constant)
  - PGC_S_TEST (enum value)
  - NOTICE (error level)
- Called from (representative examples):
  - GUC system (referenced in guc_hooks.h)

## Notes and Other Information
- This function is registered as a check hook for the default_table_access_method GUC parameter
- Validation is context-dependent: full validation only occurs when inside a transaction and connected to a database
- For test scenarios (PGC_S_TEST), nonexistent access methods generate a NOTICE rather than an error
- The function ensures the access method name does not exceed NAMEDATALEN characters (typically 64 bytes)
- Returns true if the value is valid, false otherwise
- Part of PostgreSQL's pluggable table access method infrastructure