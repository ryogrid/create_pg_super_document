# check_transaction_read_only

## Location
[src/backend/commands/variable.c:544-582](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/variable.c#L544-L582)

## Overview
This function validates changes to the transaction read-only mode setting, enforcing PostgreSQL's transaction isolation rules and preventing invalid mode transitions.

## Definition

```c
bool
check_transaction_read_only(bool *newval, void **extra, GucSource source)
```
## Detailed Description
 is a GUC (Grand Unified Configuration) check hook function that validates attempts to change the transaction read-only mode via  or  commands. The function implements PostgreSQL's transaction isolation semantics by allowing idempotent changes and read-write to read-only transitions at any time, while strictly controlling when read-only transactions can be changed to read-write mode.

The function enforces several key restrictions:
1. Read-only transactions cannot be changed to read-write within subtransactions
2. Top-level transactions cannot switch from read-only to read-write after taking their first snapshot
3. Transactions cannot be set to read-write mode during recovery (hot standby mode)

When not in an active transaction, all changes are permitted since  will be reset by the next .

## Parameters / Member Variables
- `*newval`: Pointer to the new boolean value for the transaction read-only setting (false = read-write, true = read-only)
- `**extra`: Pointer to extra data (unused in this function, can be NULL)
- `source`: The source of the configuration change (GucSource enum)
## Dependencies
- Functions called/Symbols referenced:
  - [IsTransactionState](../I/IsTransactionState.md)
  - [IsSubTransaction](../I/IsSubTransaction.md)
  - [GUC_check_errcode](../G/GUC_check_errcode.md)
  - GUC_check_errmsg
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
  - GucSource (enum type)
- Called from (representative examples):
  - GUC system via function pointer in guc_hooks.h

## Notes and Other Information
- This is a GUC check hook function, part of PostgreSQL's configuration validation system
- The function uses global variables , , and  to determine transaction state
- Error messages are set using  and  for proper error reporting
- Returns  for valid transitions,  for invalid ones
- Special handling for parallel worker initialization where changes are always allowed
- The function is registered in the GUC system to be called whenever the transaction_read_only setting is modified

## Simplified Source

```c
bool
check_transaction_read_only(bool *newval, void **extra, GucSource source)
{
    // Allow any change if not changing to read-write or not in transaction
    if (*newval == false && XactReadOnly && IsTransactionState() && !InitializingParallelWorker)
    {
        // Prevent read-write mode in subtransactions
        if (IsSubTransaction())
        {
            GUC_check_errcode(ERRCODE_ACTIVE_SQL_TRANSACTION);
            GUC_check_errmsg("cannot set transaction read-write mode inside a read-only transaction");
            return false;
        }

        // Prevent read-write mode after first snapshot
        if (FirstSnapshotSet)
        {
            GUC_check_errcode(ERRCODE_ACTIVE_SQL_TRANSACTION);
            GUC_check_errmsg("transaction read-write mode must be set before any query");
            return false;
        }

        // Prevent read-write mode during recovery
        if (RecoveryInProgress())
        {
            GUC_check_errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
            GUC_check_errmsg("cannot set transaction read-write mode during recovery");
            return false;
        }
    }

    return true;
}
```