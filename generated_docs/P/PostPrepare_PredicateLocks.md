# PostPrepare_PredicateLocks

## Location
[src/backend/storage/lmgr/predicate.c:4849-4871](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L4849-L4871)

## Overview
Cleans up local predicate lock state after successful transaction preparation in two-phase commit, while preserving the global serializable transaction structure.

## Definition
```c
void PostPrepare_PredicateLocks(TransactionId xid)
```

## Detailed Description
This function performs cleanup operations after a serializable transaction has been successfully prepared for two-phase commit. Unlike the regular lock manager which needs to transfer locks to a dummy PGPROC, the predicate lock system keeps the SERIALIZABLEXACT structure intact since it needs to remain available for conflict detection with other transactions even after preparation.

The cleanup involves:
1. Clearing the process-specific identifiers (pid and pgprocno) from the serializable transaction since the preparing process is no longer associated with it
2. Destroying the local predicate lock hash table since it's no longer needed - the global predicate lock information is preserved in the SERIALIZABLEXACT structure
3. Resetting local transaction state variables to indicate this process is no longer involved in a serializable transaction

This approach allows the serializable transaction to remain visible for conflict detection while freeing up local resources that are no longer needed after preparation.

## Parameters / Member Variables
- `xid`: The transaction ID of the prepared transaction (currently not used in the implementation but provided for interface consistency)

## Dependencies
- Functions called/Symbols referenced:
  - SxactIsPrepared: Verifies the transaction is in prepared state
  - hash_destroy: Destroys the LocalPredicateLockHash table
  - INVALID_PROC_NUMBER: Constant for invalid process number
  - InvalidSerializableXact: Constant representing no serializable transaction
- Called from (representative examples):
  - PrepareTransaction: After successful transaction preparation

## Notes and Other Information
- Only operates on serializable transactions (returns early if MySerializableXact is invalid)
- Includes assertion to verify the transaction is actually prepared before cleanup
- Unlike regular lock managers, does not transfer locks to dummy processes since SERIALIZABLEXACT persists
- The transaction ID parameter is currently unused but maintains interface consistency
- Local predicate lock hash destruction is safe since global predicate lock state is preserved
- After this function, the preparing backend is no longer associated with the serializable transaction