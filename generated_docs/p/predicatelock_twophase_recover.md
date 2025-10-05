# predicatelock_twophase_recover

## Location
[src/backend/storage/lmgr/predicate.c:4899-5035](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L4899-L5035)

## Overview
Reconstructs serializable transaction state and predicate locks during recovery from two-phase commit state file records.

## Definition
```c
void predicatelock_twophase_recover(TransactionId xid, uint16 info, void *recdata, uint32 len)
```

## Detailed Description
This function is the recovery counterpart to AtPrepare_PredicateLocks, responsible for reconstructing the serializable transaction state from 2PC state file records during database recovery. It processes two types of records created during prepare:

**Transaction Record Recovery (TWOPHASEPREDICATERECORD_XACT):**
- Creates a new SERIALIZABLEXACT structure using CreatePredXact()
- Sets up the transaction with a special vxid (INVALID_PROC_NUMBER/xid) since no actual process is associated during recovery
- Initializes conflict lists and conservatively assumes the transaction had both incoming and outgoing conflicts by setting summary conflict flags
- Updates global transaction state including SxactGlobalXmin and WritableSxactCount
- Registers the transaction in SerializableXidHash for later lookup

**Lock Record Recovery (TWOPHASEPREDICATERECORD_LOCK):**
- Recreates individual predicate locks by calling CreatePredicateLock()
- Looks up the previously recovered SERIALIZABLEXACT by transaction ID
- Associates each recovered lock with the correct serializable transaction

The conservative approach during recovery ensures serialization safety by assuming conflicts existed even if they weren't explicitly recorded, since conflicts can be added after preparation.

## Parameters / Member Variables
- `xid`: Transaction ID of the prepared transaction being recovered
- `info`: Additional information from 2PC record (currently unused)
- `recdata`: Pointer to the TwoPhasePredicateRecord containing serialized state
- `len`: Length of the record data (validated to match expected size)

## Dependencies
- Functions called/Symbols referenced:
  - [CreatePredXact](../C/CreatePredXact.md): Creates new SERIALIZABLEXACT structure
  - [CreatePredicateLock](../C/CreatePredicateLock.md): Recreates individual predicate locks
  - PredicateLockTargetTagHashCode: Computes hash for lock targets
  - [hash_search](../h/hash_search.md): Searches/inserts into SerializableXidHash
  - [SerialSetActiveSerXmin](../S/SerialSetActiveSerXmin.md): Updates global minimum transaction ID
  - [TransactionIdFollows](../T/TransactionIdFollows.md)/TransactionIdEquals: Transaction ID comparison utilities
  - [dlist_init](../d/dlist_init.md)/dlist_node_init: Initialize list structures
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease: Lock management
- Called from (representative examples):
  - Two-phase commit recovery system during database startup

## Notes and Other Information
- Handles both transaction and lock record types within a single function
- Uses conservative conflict assumptions (SXACT_FLAG_SUMMARY_CONFLICT_IN/OUT) to ensure safety during recovery
- Special handling for global xmin updates during recovery allows backwards movement
- Prepared transactions during recovery have no associated process (pid = 0, pgprocno = INVALID_PROC_NUMBER)
- Critical for maintaining serializable isolation across database restarts when prepared transactions exist
- The function includes assertions to validate record structure and transaction state consistency

## Simplified Source

```c
void predicatelock_twophase_recover(TransactionId xid, uint16 info,
                                   void *recdata, uint32 len)
{
    TwoPhasePredicateRecord *record;

    Assert(len == sizeof(TwoPhasePredicateRecord));
    record = (TwoPhasePredicateRecord *) recdata;

    if (record->type == TWOPHASEPREDICATERECORD_XACT)
    {
        // Recover transaction record - create SERIALIZABLEXACT
        TwoPhasePredicateXactRecord *xactRecord;
        SERIALIZABLEXACT *sxact;
        SERIALIZABLEXID *sxid;
        SERIALIZABLEXIDTAG sxidtag;
        bool found;

        xactRecord = (TwoPhasePredicateXactRecord *) &record->data.xactRecord;

        LWLockAcquire(SerializableXactHashLock, LW_EXCLUSIVE);
        sxact = CreatePredXact();
        if (!sxact)
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
                           errmsg("out of shared memory")));

        // Set up transaction with special vxid for prepared state
        sxact->vxid.procNumber = INVALID_PROC_NUMBER;
        sxact->vxid.localTransactionId = (LocalTransactionId) xid;
        sxact->pid = 0;
        sxact->pgprocno = INVALID_PROC_NUMBER;

        // Initialize transaction state
        sxact->prepareSeqNo = RecoverySerCommitSeqNo;
        sxact->commitSeqNo = InvalidSerCommitSeqNo;
        sxact->finishedBefore = InvalidTransactionId;
        sxact->SeqNo.lastCommitBeforeSnapshot = RecoverySerCommitSeqNo;

        // Initialize lists and set transaction data
        dlist_init(&(sxact->possibleUnsafeConflicts));
        dlist_init(&(sxact->predicateLocks));
        dlist_node_init(&sxact->finishedLink);
        sxact->topXid = xid;
        sxact->xmin = xactRecord->xmin;
        sxact->flags = xactRecord->flags;

        if (!SxactIsReadOnly(sxact))
            ++(PredXact->WritableSxactCount);

        // Conservatively assume conflicts existed
        dlist_init(&(sxact->outConflicts));
        dlist_init(&(sxact->inConflicts));
        sxact->flags |= SXACT_FLAG_SUMMARY_CONFLICT_IN;
        sxact->flags |= SXACT_FLAG_SUMMARY_CONFLICT_OUT;

        // Register transaction in hash table
        sxidtag.xid = xid;
        sxid = (SERIALIZABLEXID *) hash_search(SerializableXidHash,
                                              &sxidtag, HASH_ENTER, &found);
        Assert(!found);
        sxid->myXact = (SERIALIZABLEXACT *) sxact;

        // Update global xmin
        if (!TransactionIdIsValid(PredXact->SxactGlobalXmin) ||
            TransactionIdFollows(PredXact->SxactGlobalXmin, sxact->xmin))
        {
            PredXact->SxactGlobalXmin = sxact->xmin;
            PredXact->SxactGlobalXminCount = 1;
            SerialSetActiveSerXmin(sxact->xmin);
        }
        else if (TransactionIdEquals(sxact->xmin, PredXact->SxactGlobalXmin))
        {
            PredXact->SxactGlobalXminCount++;
        }

        LWLockRelease(SerializableXactHashLock);
    }
    else if (record->type == TWOPHASEPREDICATERECORD_LOCK)
    {
        // Recover lock record - recreate PREDICATELOCK
        TwoPhasePredicateLockRecord *lockRecord;
        SERIALIZABLEXID *sxid;
        SERIALIZABLEXACT *sxact;
        SERIALIZABLEXIDTAG sxidtag;
        uint32 targettaghash;

        lockRecord = (TwoPhasePredicateLockRecord *) &record->data.lockRecord;
        targettaghash = PredicateLockTargetTagHashCode(&lockRecord->target);

        // Find the transaction this lock belongs to
        LWLockAcquire(SerializableXactHashLock, LW_SHARED);
        sxidtag.xid = xid;
        sxid = (SERIALIZABLEXID *)
            hash_search(SerializableXidHash, &sxidtag, HASH_FIND, NULL);
        LWLockRelease(SerializableXactHashLock);

        Assert(sxid != NULL);
        sxact = sxid->myXact;
        Assert(sxact != InvalidSerializableXact);

        // Recreate the predicate lock
        CreatePredicateLock(&lockRecord->target, targettaghash, sxact);
    }
}
```