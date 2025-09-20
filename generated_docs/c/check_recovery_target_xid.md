# check_recovery_target_xid

## Location
[src/backend/access/transam/xlogrecovery.c:5012-5034](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L5012-L5034)

## Overview
A GUC check hook function that validates recovery_target_xid parameter values, ensuring they are valid transaction IDs.

## Definition

```c
bool
check_recovery_target_xid(char **newval, void **extra, GucSource source)
```
## Detailed Description
This function serves as a GUC check hook for the  parameter. It validates that the provided value is a valid transaction ID (XID) by parsing it as a 64-bit unsigned integer using . If the value is non-empty, it performs numeric validation and stores the parsed TransactionId in dynamically allocated memory for later use by the assign hook. Empty values are accepted and result in no extra data being stored, allowing the parameter to be unset.

## Parameters / Member Variables
- : Pointer to the new value string for recovery_target_xid (transaction ID or empty string)
- : Pointer to store the parsed TransactionId for the assign hook
- : The source of the GUC setting (configuration file, command line, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - strtou64 (for parsing 64-bit unsigned integers)
  - [guc_malloc](../g/guc_malloc.md) (for memory allocation)
  - TransactionId (type definition)
  - GucSource (enum type)
- Called from (representative examples):
  - GUC system (via function pointer in GUC_HOOKS_H)

## Notes and Other Information
- This function is part of PostgreSQL's point-in-time recovery (PITR) system
- Transaction IDs are 64-bit unsigned integers in modern PostgreSQL versions
- Returns false if the XID value is invalid, causing the GUC assignment to fail
- Allocates memory to store the validated TransactionId for the assign hook
- Empty string values are valid and indicate the recovery target XID should be unset
- Located in src/backend/access/transam/xlogrecovery.c:5012-5034