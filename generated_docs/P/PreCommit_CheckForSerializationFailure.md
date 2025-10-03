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

## Dependencies
- Functions called/Symbols referenced:
  - IsolationIsSerializable: Checks if current isolation level is serializable
  - SxactIsDoomed: Checks if a transaction is marked for rollback
  - SxactIsPartiallyReleased: Checks if transaction has been partially cleaned up
  - SxactIsCommitted: Checks if a transaction has committed
  - SxactIsReadOnly: Checks if a transaction is read-only
  - SxactIsPrepared: Checks if a transaction is prepared (two-phase commit)
  - dlist_foreach/dlist_container: List iteration utilities
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease: Lock management for SerializableXactHashLock
  - ereport: Error reporting mechanism
- Called from (representative examples):
  - [CommitTransaction](../C/CommitTransaction.md): During normal transaction commit
  - [PrepareTransaction](PrepareTransaction.md): During two-phase commit preparation

## Notes and Other Information
- This function is critical for maintaining serializability in PostgreSQL's SSI implementation
- It uses a "first committer wins" strategy where the committing transaction takes precedence over conflicting transactions
- The function operates under the SerializableXactHashLock to ensure consistency when examining conflict structures
- Error codes ERRCODE_T_R_SERIALIZATION_FAILURE are used to signal serialization failures to applications
- The prepareSeqNo assignment and SXACT_FLAG_PREPARED setting at the end mark this transaction as committed in the serialization conflict graph

## Simplified Source

```c
void PreCommit_CheckForSerializationFailure(void)
{
    // Early exit if not a serializable transaction
    if (MySerializableXact == InvalidSerializableXact)
        return;

    Assert(IsolationIsSerializable());

    LWLockAcquire(SerializableXactHashLock, LW_EXCLUSIVE);

    // Check if we've been marked for death by another transaction
    if (SxactIsDoomed(MySerializableXact) &&
        !SxactIsPartiallyReleased(MySerializableXact)) {
        LWLockRelease(SerializableXactHashLock);
        ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                errmsg("could not serialize access due to read/write dependencies among transactions"),
                errdetail_internal("Reason code: Canceled on identification as a pivot, during commit attempt."),
                errhint("The transaction might succeed if retried.")));
    }

    // Look for dangerous conflict structures (triangular patterns)
    dlist_foreach(near_iter, &MySerializableXact->inConflicts) {
        RWConflict nearConflict = dlist_container(RWConflictData, inLink, near_iter.cur);

        // Skip if the near conflict has already committed or been doomed
        if (!SxactIsCommitted(nearConflict->sxactOut) &&
            !SxactIsDoomed(nearConflict->sxactOut)) {

            // Check for far conflicts that complete the dangerous structure
            dlist_foreach(far_iter, &nearConflict->sxactOut->inConflicts) {
                RWConflict farConflict = dlist_container(RWConflictData, inLink, far_iter.cur);

                if (farConflict->sxactOut == MySerializableXact ||
                    (!SxactIsCommitted(farConflict->sxactOut) &&
                     !SxactIsReadOnly(farConflict->sxactOut) &&
                     !SxactIsDoomed(farConflict->sxactOut))) {

                    // Found dangerous structure - handle it
                    if (SxactIsPrepared(nearConflict->sxactOut)) {
                        // Can't kill prepared transaction, so we abort instead
                        LWLockRelease(SerializableXactHashLock);
                        ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                                errmsg("could not serialize access due to read/write dependencies among transactions"),
                                errdetail_internal("Reason code: Canceled on commit attempt with conflict in from prepared pivot."),
                                errhint("The transaction might succeed if retried.")));
                    }

                    // Mark pivot transaction for rollback
                    nearConflict->sxactOut->flags |= SXACT_FLAG_DOOMED;
                    break;
                }
            }
        }
    }

    // Mark this transaction as prepared in the conflict graph
    MySerializableXact->prepareSeqNo = ++(PredXact->LastSxactCommitSeqNo);
    MySerializableXact->flags |= SXACT_FLAG_PREPARED;

    LWLockRelease(SerializableXactHashLock);
}
```