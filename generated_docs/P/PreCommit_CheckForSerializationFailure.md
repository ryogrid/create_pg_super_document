# PreCommit_CheckForSerializationFailure

## Location
[src/backend/storage/lmgr/predicate.c:4693-4779](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L4693-L4779)

## Overview
Checks for dangerous serialization conflict structures in a serializable transaction at commit time and handles serialization failures by marking pivot transactions for rollback.

## Definition
```c
void PreCommit_CheckForSerializationFailure(void)
```

## Detailed Description
This function is called before committing a serializable transaction to detect dangerous conflict patterns that could lead to serialization anomalies. It implements PostgreSQL's Serializable Snapshot Isolation (SSI) by looking for "dangerous structures" - specifically read-write conflict cycles that form triangular patterns between transactions.

The function examines all incoming conflicts (transactions that this transaction conflicts with) and checks if those transactions also have their own incoming conflicts that could complete a dangerous cycle. When such a structure is found, it marks the "pivot" transaction (the middle transaction in the conflict chain) as doomed, forcing it to abort. This ensures forward progress by allowing the committing transaction to succeed while preventing potential serialization anomalies.

If the current transaction has already been marked as doomed by another transaction, it will abort with a serialization failure error. Special handling is provided for prepared transactions - if a pivot transaction is already prepared (in a two-phase commit), the current transaction aborts instead since prepared transactions cannot be easily rolled back.

## Parameters / Member Variables
This function takes no parameters and operates on global transaction state.

## Dependencies
- Functions called/Symbols referenced:
  - IsolationIsSerializable: Checks if current isolation level is serializable
  - SxactIsDoomed: Checks if a transaction is marked for rollback
  - SxactIsPartiallyReleased: Checks if transaction has been partially cleaned up
  - SxactIsCommitted: Checks if a transaction has committed
  - SxactIsReadOnly: Checks if a transaction is read-only
  - SxactIsPrepared: Checks if a transaction is prepared (two-phase commit)
  - dlist_foreach/dlist_container: List iteration utilities
  - LWLockAcquire/LWLockRelease: Lock management for SerializableXactHashLock
  - ereport: Error reporting mechanism
- Called from (representative examples):
  - CommitTransaction: During normal transaction commit
  - PrepareTransaction: During two-phase commit preparation

## Notes and Other Information
- This function is critical for maintaining serializability in PostgreSQL's SSI implementation
- It uses a "first committer wins" strategy where the committing transaction takes precedence over conflicting transactions
- The function operates under the SerializableXactHashLock to ensure consistency when examining conflict structures
- Error codes ERRCODE_T_R_SERIALIZATION_FAILURE are used to signal serialization failures to applications
- The prepareSeqNo assignment and SXACT_FLAG_PREPARED setting at the end mark this transaction as committed in the serialization conflict graph