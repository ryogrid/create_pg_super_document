# GetSerializableTransactionSnapshotInt

## Location
[src/backend/storage/lmgr/predicate.c:1754-1929](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L1754-L1929)

## Overview
Core internal implementation function that handles the actual setup of serializable transaction context, creating and initializing the SERIALIZABLEXACT structure with snapshot data and conflict tracking mechanisms.

## Definition
```c
static Snapshot GetSerializableTransactionSnapshotInt(Snapshot snapshot, VirtualTransactionId *sourcevxid, int sourcepid)
```

## Detailed Description
This is the main internal implementation function for both GetSerializableTransactionSnapshot and SetSerializableTransactionSnapshot. It performs the complex initialization required for serializable transactions including:

1. **Transaction Structure Creation**: Creates and initializes a SERIALIZABLEXACT structure to track this transaction's serializable state
2. **Snapshot Handling**: Either takes a new snapshot via GetSnapshotData() or validates an imported snapshot
3. **Read-Only Optimizations**: Implements several optimizations for read-only transactions, allowing them to "opt out" of predicate locking when safe
4. **Conflict Detection Setup**: For read-only transactions, registers all concurrent read-write transactions as possible unsafe conflicts
5. **Global State Management**: Maintains global serializable transaction state including global xmin tracking

The function includes sophisticated race condition handling, particularly for imported snapshots where it must verify the source transaction is still running. It also implements performance optimizations that allow read-only transactions to bypass the overhead of predicate locking when no dangerous patterns exist.

## Parameters / Member Variables
- `snapshot`: The snapshot structure to use (either to be filled by GetSnapshotData or already populated for import)
- `sourcevxid`: Virtual transaction ID of source transaction (NULL for new snapshots, valid for imported snapshots)
- `sourcepid`: Process ID of source transaction (used for error reporting in import scenarios)

## Dependencies
- Functions called/Symbols referenced:
  - [CreatePredXact](../C/CreatePredXact.md) (creates new SERIALIZABLEXACT structure)
  - [GetSnapshotData](GetSnapshotData.md) (obtains new snapshot data)
  - [ProcArrayInstallImportedXmin](../P/ProcArrayInstallImportedXmin.md) (validates imported snapshots)
  - [GetTopTransactionIdIfAny](GetTopTransactionIdIfAny.md) (gets current transaction ID if assigned)
  - [CreateLocalPredicateLockHash](../C/CreateLocalPredicateLockHash.md) (initializes local predicate lock tracking)
  - Various conflict tracking functions (SetPossibleUnsafeConflict, SxactIsCommitted, etc.)
  - [TransactionIdFollows](../T/TransactionIdFollows.md), TransactionIdEquals (transaction ID comparison utilities)
- Called from (representative examples):
  - [GetSerializableTransactionSnapshot](GetSerializableTransactionSnapshot.md) (for new snapshots)
  - [SetSerializableTransactionSnapshot](../S/SetSerializableTransactionSnapshot.md) (for imported snapshots)
  - [GetSafeSnapshot](GetSafeSnapshot.md) (for deferrable read-only transactions)

## Notes and Other Information
- Static function - only used internally within predicate.c
- Cannot be called during parallel operations as all parts of a serializable transaction must use the same snapshot
- Includes sophisticated "opt-out" logic for read-only transactions when no write/write conflicts are possible
- Maintains complex global state including WritableSxactCount and SxactGlobalXmin
- Handles memory management by potentially calling SummarizeOldestCommittedSxact() to free old transaction structures
- Critical for PostgreSQL's Serializable Snapshot Isolation (SSI) implementation
- Uses extensive locking (SerializableXactHashLock) to prevent race conditions during initialization

## Simplified Source

```c
// Simplified version of GetSerializableTransactionSnapshotInt
static Snapshot
GetSerializableTransactionSnapshotInt(Snapshot snapshot,
                                     VirtualTransactionId *sourcevxid,
                                     int sourcepid)
{
    PGPROC *proc;
    VirtualTransactionId vxid;
    SERIALIZABLEXACT *sxact, *othersxact;

    // Basic validation - must be serializable transaction, not in parallel mode
    Assert(MySerializableXact == InvalidSerializableXact);
    Assert(!RecoveryInProgress());

    if (IsInParallelMode())
        elog(ERROR, "cannot establish serializable snapshot during parallel operation");

    proc = MyProc;
    GET_VXID_FROM_PGPROC(vxid, *proc);

    // Create serializable transaction structure, retry if memory full
    LWLockAcquire(SerializableXactHashLock, LW_EXCLUSIVE);
    do {
        sxact = CreatePredXact();
        if (!sxact) {
            // Free old committed transactions and retry
            LWLockRelease(SerializableXactHashLock);
            SummarizeOldestCommittedSxact();
            LWLockAcquire(SerializableXactHashLock, LW_EXCLUSIVE);
        }
    } while (!sxact);

    // Get snapshot data or validate imported snapshot
    if (!sourcevxid) {
        snapshot = GetSnapshotData(snapshot);
    } else if (!ProcArrayInstallImportedXmin(snapshot->xmin, sourcevxid)) {
        // Source transaction no longer running
        ReleasePredXact(sxact);
        LWLockRelease(SerializableXactHashLock);
        ereport(ERROR, "could not import the requested snapshot");
    }

    // Read-only optimization: if no writable transactions, skip tracking
    if (XactReadOnly && PredXact->WritableSxactCount == 0) {
        ReleasePredXact(sxact);
        LWLockRelease(SerializableXactHashLock);
        return snapshot;
    }

    // Initialize serializable transaction structure
    sxact->vxid = vxid;
    sxact->SeqNo.lastCommitBeforeSnapshot = PredXact->LastSxactCommitSeqNo;
    sxact->topXid = GetTopTransactionIdIfAny();
    sxact->xmin = snapshot->xmin;
    sxact->pid = MyProcPid;
    sxact->pgprocno = MyProcNumber;

    // Initialize conflict tracking lists
    dlist_init(&(sxact->outConflicts));
    dlist_init(&(sxact->inConflicts));
    dlist_init(&(sxact->possibleUnsafeConflicts));
    dlist_init(&sxact->predicateLocks);

    if (XactReadOnly) {
        sxact->flags |= SXACT_FLAG_READ_ONLY;

        // Register all concurrent read-write transactions as potential conflicts
        dlist_foreach(iter, &PredXact->activeList) {
            othersxact = dlist_container(SERIALIZABLEXACT, xactLink, iter.cur);
            if (!SxactIsCommitted(othersxact) && !SxactIsDoomed(othersxact) &&
                !SxactIsReadOnly(othersxact)) {
                SetPossibleUnsafeConflict(sxact, othersxact);
            }
        }

        // Another optimization: if no unsafe conflicts, skip tracking
        if (dlist_is_empty(&sxact->possibleUnsafeConflicts)) {
            ReleasePredXact(sxact);
            LWLockRelease(SerializableXactHashLock);
            return snapshot;
        }
    } else {
        // Increment count of writable transactions
        ++(PredXact->WritableSxactCount);
    }

    // Update global xmin tracking
    if (!TransactionIdIsValid(PredXact->SxactGlobalXmin)) {
        PredXact->SxactGlobalXmin = snapshot->xmin;
        PredXact->SxactGlobalXminCount = 1;
        SerialSetActiveSerXmin(snapshot->xmin);
    } else if (TransactionIdEquals(snapshot->xmin, PredXact->SxactGlobalXmin)) {
        PredXact->SxactGlobalXminCount++;
    }

    // Set global state and release lock
    MySerializableXact = sxact;
    MyXactDidWrite = false;
    LWLockRelease(SerializableXactHashLock);

    // Initialize local predicate lock hash
    CreateLocalPredicateLockHash();

    return snapshot;
}
```

Key simplifications made:
- Removed detailed error handling comments for clarity
- Consolidated memory management retry logic into simpler form
- Simplified complex conditional structures
- Abstracted low-level list operations with clear comments
- Removed test-specific code (#ifdef TEST_SUMMARIZE_SERIAL)
- Streamlined global xmin management logic
- Added high-level comments explaining each major section
- Preserved all essential algorithm steps and optimizations