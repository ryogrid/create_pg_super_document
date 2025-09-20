# check_temp_tablespaces

## Location
[src/backend/commands/tablespace.c:1198-1305](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablespace.c#L1198-L1305)

## Overview
Validates new values for the temp_tablespaces GUC parameter, ensuring all specified tablespace names exist and the user has CREATE permission on them.

## Definition

```c
struct for assign_temp_tablespaces */
		myextra = guc_malloc(LOG, offsetof(temp_tablespaces_extra, tblSpcs) +
							 numSpcs * sizeof(Oid));
```
## Detailed Description
This function serves as a check hook for the temp_tablespaces GUC variable. It parses the comma-separated list of tablespace names and validates each one. The validation process includes:

1. **Syntax validation**: Ensures the string can be parsed as a comma-separated list of identifiers
2. **Existence check**: Verifies each named tablespace exists in the system catalogs (when in a transaction)
3. **Permission validation**: Confirms the current user has CREATE permission on each tablespace
4. **Special handling**: Allows empty strings and explicit database default tablespace references

The function behaves differently based on the GUC source - for test scenarios (PGC_S_TEST), it issues NOTICEs instead of errors for missing tablespaces. When outside a transaction or not connected to a database, it accepts values on faith since catalog access isn't possible.

## Parameters
- : Pointer to the new string value for temp_tablespaces GUC
- : Output parameter where validation results are stored for later use by assign_temp_tablespaces
- : Source of the GUC change (interactive, config file, test, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - SplitIdentifierString
  - GUC_check_errdetail
  - [IsTransactionState](../I/IsTransactionState.md)
  - [get_tablespace_oid](../g/get_tablespace_oid.md)
  - [object_aclcheck](../o/object_aclcheck.md)
  - [aclcheck_error](../a/aclcheck_error.md)
  - [guc_malloc](../g/guc_malloc.md)
  - [list_free](../l/list_free.md)
- Called from (representative examples):
  - GUC system (referenced in src/include/utils/guc_hooks.h:157)

## Notes and Other Information
- Returns false if validation fails, true if successful
- Creates a temp_tablespaces_extra structure in *extra containing validated tablespace OIDs
- Allows empty tablespace names which signify the database's default tablespace
- Converts explicit database default tablespace references to InvalidOid for consistency
- Different error handling based on GUC source: hard errors for interactive use, notices for tests
- Memory allocated for the extra structure uses guc_malloc with LOG level