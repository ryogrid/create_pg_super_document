# check_transaction_deferrable

## Location
[src/backend/commands/variable.c:620-647](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/variable.c#L620-L647)

## Overview
This function validates changes to the transaction deferrable setting, ensuring that deferrable mode modifications follow PostgreSQL's transaction timing constraints.

## Definition

```c
bool
check_transaction_deferrable(bool *newval, void **extra, GucSource source)
```
## Detailed Description
 is a GUC check hook function that validates attempts to change the transaction deferrable setting via  or  commands. The deferrable property is only meaningful for serializable transactions and allows them to be deferred until they can run without causing serialization failures.

The function enforces two key restrictions:
1. The deferrable setting cannot be changed within subtransactions
2. The deferrable setting must be set before any query execution (before the first snapshot is taken)

These restrictions ensure that the deferrable property is set at the appropriate transaction scope and timing.

## Parameters / Member Variables
- `*newval`: Pointer to the new boolean value for the transaction deferrable setting (true = deferrable, false = not deferrable)
- `**extra`: Pointer to extra data (unused in this function, can be NULL)
- `source`: The source of the configuration change (GucSource enum)
## Dependencies
- Functions called/Symbols referenced:
  - [IsSubTransaction](../I/IsSubTransaction.md)
  - [GUC_check_errcode](../G/GUC_check_errcode.md)
  - GUC_check_errmsg
  - GucSource (enum type)
- Called from (representative examples):
  - GUC system via function pointer in guc_hooks.h

## Notes and Other Information
- This is a GUC check hook function for the  configuration parameter
- The deferrable property is primarily useful for serializable transactions to avoid serialization conflicts
- Uses the global variable  to determine if queries have already been executed
- Returns  for valid deferrable setting changes,  for invalid ones
- Simpler validation logic compared to other transaction-related check functions since it doesn't need to handle recovery mode or idempotent changes
- Error codes used are ERRCODE_ACTIVE_SQL_TRANSACTION for both restriction types

## Simplified Source

```c
bool
check_transaction_deferrable(bool *newval, void **extra, GucSource source)
{
    // Prevent changes in subtransactions
    if (IsSubTransaction())
    {
        GUC_check_errcode(ERRCODE_ACTIVE_SQL_TRANSACTION);
        GUC_check_errmsg("SET TRANSACTION [NOT] DEFERRABLE cannot be called within a subtransaction");
        return false;
    }

    // Prevent changes after first snapshot
    if (FirstSnapshotSet)
    {
        GUC_check_errcode(ERRCODE_ACTIVE_SQL_TRANSACTION);
        GUC_check_errmsg("SET TRANSACTION [NOT] DEFERRABLE must be called before any query");
        return false;
    }

    return true;
}
```