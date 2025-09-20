# removable_cutoff

## Location
[src/test/modules/injection_points/regress_injection.c:42-78](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/injection_points/regress_injection.c#L42-L78)

## Overview
A test utility function that wraps GetOldestNonRemovableTransactionId() to determine the oldest transaction ID that cannot be removed by VACUUM, primarily used for isolation testing in the injection_points test module.

## Definition

```c
Datum
removable_cutoff(PG_FUNCTION_ARGS)
```
## Detailed Description
The `removable_cutoff` function is a PostgreSQL SQL-callable function designed specifically for testing purposes, particularly for the syscache-update-pruned.spec isolation test. It serves as a wrapper around `GetOldestNonRemovableTransactionId()` with additional safeguards to handle the inherent instability of transaction ID horizons.

The function addresses a key challenge in isolation testing: the oldest non-removable transaction ID can move backward under certain conditions, which can lead to test instability. The function implements a retry mechanism to ensure consistent results by repeatedly checking the transaction ID until the next transaction ID remains stable between calls.

Key behaviors:
- Takes an optional relation OID as parameter to determine the oldest non-removable XID for that specific relation
- Implements a retry loop to handle race conditions where the transaction ID horizon changes during execution  
- Issues a warning when called on non-shared relations with autovacuum enabled, as this can cause backward movement
- Returns a FullTransactionId with proper epoch handling to avoid XID wraparound issues

The function is part of the injection_points test module and is primarily used for testing transaction visibility and pruning behavior in PostgreSQL's MVCC system.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure
  - Argument 0 (optional): OID of the relation to check. If NULL, checks global oldest non-removable XID

## Dependencies
- Functions called/Symbols referenced:
  - `table_open`: Opens a relation with specified lock mode
  - `table_close`: Closes a relation and releases lock
  - `ReadNextFullTransactionId`: Reads the next transaction ID to be assigned
  - [GetOldestNonRemovableTransactionId](../G/GetOldestNonRemovableTransactionId.md): Core function to determine oldest non-removable XID
  - `FullTransactionIdEquals`: Compares two FullTransactionId values for equality
  - `FullTransactionIdFromAllowableAt`: Creates a FullTransactionId with proper epoch
  - `PG_RETURN_FULLTRANSACTIONID`: Returns a FullTransactionId to the SQL caller
  - `CHECK_FOR_INTERRUPTS`: Allows for query cancellation during retry loop

- Called from (representative examples):
  - No direct callers found (test utility function called from SQL)

## Notes and Other Information
- This function is located in src/test/modules/injection_points/regress_injection.c:42-78
- Designed specifically for isolation testing and should not be used in production code
- The retry mechanism is necessary because the oldest removable XID can change concurrently with other database operations
- The function includes specific handling for shared vs non-shared relations, with warnings about autovacuum interactions
- Part of the broader injection_points test framework for PostgreSQL regression testing
- The comment references a mailing list discussion about causes of backward movement in transaction horizons
- Uses AccessShareLock when opening relations to minimize interference with concurrent operations