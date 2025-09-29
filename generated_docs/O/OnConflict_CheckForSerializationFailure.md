# OnConflict_CheckForSerializationFailure

## Location
[src/backend/storage/lmgr/predicate.c:4526-4692](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L4526-L4692)

## Overview
OnConflict_CheckForSerializationFailure detects dangerous dependency structures in the serialization graph and aborts transactions to prevent serialization anomalies.

## Definition

```c
structure, so we must abort. (Since the writer
	 * has already committed, we must be the reader)
	 *------------------------------------------------------------------------
	 */
	if (SxactIsCommitted(writer)
		&& (SxactHasConflictOut(writer) || SxactHasSummaryConflictOut(writer)))
		failure = true;
```
## Detailed Description
This critical static function implements the core logic for detecting serialization failures in PostgreSQL's Serializable Snapshot Isolation (SSI). It analyzes dependency graphs to identify dangerous structures that could lead to serialization anomalies and takes corrective action by aborting appropriate transactions.

The function detects three main dangerous patterns:

**Pattern 1: Committed Writer with Outbound Conflict**

When the writer is already committed and has an outbound rw-conflict, this creates a dangerous structure requiring reader abortion.

**Pattern 2: Writer as Pivot with Later Committer**

The writer becomes a dangerous pivot when T2 commits first, checked through sequence number comparisons and transaction state analysis.

**Pattern 3: Reader as Pivot with Prepared Writer**

When the writer is prepared/committed and reader has inbound conflicts, this creates a dangerous pivot requiring transaction abortion.

The function includes sophisticated optimizations for READ ONLY transactions and uses sequence number comparisons to determine commit ordering. When a dangerous structure is detected, it aborts the appropriate transaction: the current transaction if it's the writer, or flags the writer for termination, with special handling for prepared transactions.

## Parameters / Member Variables
- : Pointer to the SERIALIZABLEXACT structure of the transaction that performed the read operation creating the potential conflict
- : Pointer to the SERIALIZABLEXACT structure of the transaction that performed the write operation creating the potential conflict

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockHeldByMe](../L/LWLockHeldByMe.md)
  - SxactIsCommitted
  - SxactHasConflictOut
  - SxactHasSummaryConflictOut
  - SxactHasSummaryConflictIn
  - SxactIsPrepared
  - SxactIsReadOnly
  - SxactIsDoomed
  - dlist_foreach
  - dlist_container
  - ereport
- Called from (representative examples):
  - [FlagRWConflict](../F/FlagRWConflict.md)

## Notes and Other Information
- This is a static function internal to predicate.c, implementing the heart of SSI anomaly prevention
- Caller must hold SerializableXactHashLock (enforced by assertion)
- Uses sophisticated sequence number analysis to determine transaction commit ordering
- Handles three distinct abort scenarios: abort self, flag writer for abort, or abort self when writer is prepared
- Includes optimizations for READ ONLY transactions using lastCommitBeforeSnapshot comparisons
- Critical for maintaining serializability guarantees in PostgreSQL's SSI implementation
- Uses unconstify() macro to work around const constraints in dlist iteration
- Located in src/backend/storage/lmgr/predicate.c:4526-4692

## Simplified Source

```c
static void
OnConflict_CheckForSerializationFailure(const SERIALIZABLEXACT *reader,
                                      SERIALIZABLEXACT *writer)
{
    bool failure = false;

    // Check Pattern 1: Already committed writer with outbound conflict
    if (SxactIsCommitted(writer) &&
        (SxactHasConflictOut(writer) || SxactHasSummaryConflictOut(writer))) {
        failure = true;
    }

    // Check Pattern 2: Writer as pivot with outbound conflicts
    if (!failure && SxactHasSummaryConflictOut(writer)) {
        failure = true;
    } else if (!failure) {
        // Check individual outbound conflicts from writer
        dlist_iter iter;
        dlist_foreach(iter, &writer->outConflicts) {
            RWConflict conflict = dlist_container(RWConflictData, outLink, iter.cur);
            SERIALIZABLEXACT *t2 = conflict->sxactIn;

            // Check if T2 creates dangerous structure based on commit order
            if (SxactIsPrepared(t2) &&
                (!SxactIsCommitted(reader) || t2->prepareSeqNo <= reader->commitSeqNo) &&
                (!SxactIsCommitted(writer) || t2->prepareSeqNo <= writer->commitSeqNo) &&
                (!SxactIsReadOnly(reader) || t2->prepareSeqNo <= reader->SeqNo.lastCommitBeforeSnapshot)) {
                failure = true;
                break;
            }
        }
    }

    // Check Pattern 3: Reader as pivot with prepared writer
    if (!failure && SxactIsPrepared(writer) && !SxactIsReadOnly(reader)) {
        if (SxactHasSummaryConflictIn(reader)) {
            failure = true;
        } else {
            // Check individual inbound conflicts to reader
            dlist_iter iter;
            dlist_foreach(iter, &reader->inConflicts) {
                const RWConflict conflict = dlist_container(RWConflictData, inLink, iter.cur);
                const SERIALIZABLEXACT *t0 = conflict->sxactOut;

                if (!SxactIsDoomed(t0) &&
                    (!SxactIsCommitted(t0) || t0->commitSeqNo >= writer->prepareSeqNo) &&
                    (!SxactIsReadOnly(t0) || t0->SeqNo.lastCommitBeforeSnapshot >= writer->prepareSeqNo)) {
                    failure = true;
                    break;
                }
            }
        }
    }

    // Handle detected serialization failure
    if (failure) {
        if (MySerializableXact == writer) {
            // Abort current transaction (writer)
            LWLockRelease(SerializableXactHashLock);
            ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                           errmsg("could not serialize access due to read/write dependencies")));
        } else if (SxactIsPrepared(writer)) {
            // Writer is prepared, must abort reader instead
            LWLockRelease(SerializableXactHashLock);
            ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                           errmsg("could not serialize access due to read/write dependencies")));
        }
        // Flag writer for termination
        writer->flags |= SXACT_FLAG_DOOMED;
    }
}
```