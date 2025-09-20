# check_synchronous_standby_names

## Location
[src/backend/replication/syncrep.c:1058-1114](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/syncrep.c#L1058-L1114)

## Overview
A GUC (Grand Unified Configuration) check hook function that validates the syntax and semantics of the synchronous_standby_names configuration parameter when it is being set.

## Definition

```c
bool
check_synchronous_standby_names(char **newval, void **extra, GucSource source)
```
## Detailed Description
This function serves as a validation hook for the synchronous_standby_names GUC parameter in PostgreSQL. When a user attempts to set or change the synchronous_standby_names configuration, this function is called to parse and validate the new value before it is accepted. It uses a dedicated parser (syncrep_yyparse) to analyze the configuration string and ensures that the syntax is correct and the semantic requirements are met (e.g., number of synchronous standbys must be greater than zero).

The function handles the complete parsing workflow: initializing the scanner, parsing the configuration string, validating the result, and preparing the parsed configuration data for storage. If validation fails, it provides detailed error messages through the GUC error reporting mechanism.

## Parameters / Member Variables
- : Pointer to pointer containing the new value string for synchronous_standby_names parameter
- : Pointer to pointer where validated and parsed configuration data will be stored
- : Enum indicating the source of this configuration change (GucSource)

## Dependencies
- Functions called/Symbols referenced:
  - syncrep_scanner_init (initialize the synchronous replication configuration scanner)
  - syncrep_yyparse (parse the synchronous replication configuration)
  - syncrep_scanner_finish (cleanup the scanner)
  - GUC_check_errcode (set GUC error code)
  - GUC_check_errdetail (set detailed error message)
  - GUC_check_errmsg (set general error message)
  - [guc_malloc](../g/guc_malloc.md) (allocate memory using GUC's memory allocator)
- Called from (representative examples):
  - GUC system (referenced in guc_hooks.h)

## Notes and Other Information
- Returns true if validation succeeds, false if validation fails
- Uses global variables syncrep_parse_result and syncrep_parse_error_msg for parser communication
- The parsed configuration is stored in *extra as a SyncRepConfigData structure allocated with guc_malloc
- Memory management is handled automatically by the memory context system - temporary parsing results are cleaned up automatically
- If the new value is NULL or empty string, sets *extra to NULL and returns true (allowing empty configuration)
- Validates that num_sync (number of synchronous standbys) is greater than zero
- This is part of the GUC hook system that ensures configuration changes are validated before being applied