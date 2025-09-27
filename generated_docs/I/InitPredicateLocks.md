# InitPredicateLocks

## Location
[src/backend/storage/lmgr/predicate.c:1145-1346](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L1145-L1346)

## Overview
InitPredicateLocks initializes all shared memory data structures required for PostgreSQL's predicate locking system, which implements serializable snapshot isolation by tracking read-write conflicts between transactions.

## Definition
```c
void InitPredicateLocks(void)
```

## Detailed Description
This function is called during PostgreSQL startup from CreateSharedMemoryAndSemaphores() to set up the predicate locking infrastructure. It initializes several critical shared memory structures:

1. **PredicateLockTargetHash**: Hash table storing PREDICATELOCKTARGET structs with per-predicate-lock-target information. Uses partitioned locking for concurrency.

2. **PredicateLockHash**: Hash table for PREDICATELOCK structs containing per-transaction-lock-of-a-target information. Assumes an average of 2 transactions per target.

3. **PredXact**: List structure holding serializable transaction information. Assumes an average of 10 predicate locking transactions per backend for aggressive cleanup before data summarization.

4. **SerializableXidHash**: Hash table for SERIALIZABLEXID structs storing per-XID information for serializable transactions that have accessed data.

5. **RWConflictPool**: Pool for tracking read-write conflicts in lists attached to transactions. Assumes an average of 5 conflicts per transaction.

6. **FinishedSerializableTransactions**: List header for completed serializable transactions.

The function also creates a special "OldCommittedSxact" transaction representing all old committed transactions and initializes the Serial SLRU for storing historical serialization information.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - ShmemInitHash
  - [ShmemInitStruct](../S/ShmemInitStruct.md)
  - [ShmemAlloc](../S/ShmemAlloc.md)
  - [hash_search](../h/hash_search.md)
  - [CreatePredXact](../C/CreatePredXact.md)
  - [LWLockInitialize](../L/LWLockInitialize.md)
  - SetInvalidVirtualTransactionId
  - [SerialInit](../S/SerialInit.md)
  - Various dlist_* functions for doubly-linked list management
- Called from (representative examples):
  - [CreateOrAttachShmemStructs](../C/CreateOrAttachShmemStructs.md)

## Notes and Other Information
- This is a public function accessible outside predicate.c
- Critical startup function that must run during shared memory initialization
- Size calculations must agree with PredicateLockShmemSize() function
- Creates a dummy entry in PredicateLockTargetHash to ensure space is always available for page splits/combines
- Pre-calculates hash and partition lock for the scratch entry for performance
- Handles both normal postmaster and EXEC_BACKEND cases differently
- The sizing assumptions (2 xacts per target, 10 predicate locking transactions per backend, 5 conflicts per transaction) are tuned for typical workloads
- Essential component of PostgreSQL's serializable snapshot isolation implementation

## Simplified Source

```c
// Simplified version of InitPredicateLocks
void InitPredicateLocks(void) {
    HASHCTL info;
    long max_table_size;
    Size requestSize;
    bool found;

    // Step 1: Initialize PredicateLockTargetHash - stores per-predicate-lock-target info
    max_table_size = NPREDICATELOCKTARGETENTS();
    info.keysize = sizeof(PREDICATELOCKTARGETTAG);
    info.entrysize = sizeof(PREDICATELOCKTARGET);
    info.num_partitions = NUM_PREDICATELOCK_PARTITIONS;

    PredicateLockTargetHash = ShmemInitHash("PREDICATELOCKTARGET hash",
                                          max_table_size, max_table_size, &info,
                                          HASH_ELEM | HASH_BLOBS | HASH_PARTITION | HASH_FIXED_SIZE);

    // Reserve dummy entry for page splits/combines
    if (!IsUnderPostmaster) {
        hash_search(PredicateLockTargetHash, &ScratchTargetTag, HASH_ENTER, &found);
    }

    // Pre-calculate scratch entry hash for performance
    ScratchTargetTagHash = PredicateLockTargetTagHashCode(&ScratchTargetTag);
    ScratchPartitionLock = PredicateLockHashPartitionLock(ScratchTargetTagHash);

    // Step 2: Initialize PredicateLockHash - stores per-transaction-lock info
    info.keysize = sizeof(PREDICATELOCKTAG);
    info.entrysize = sizeof(PREDICATELOCK);
    info.hash = predicatelock_hash;
    max_table_size *= 2; // Assume 2 transactions per target

    PredicateLockHash = ShmemInitHash("PREDICATELOCK hash",
                                    max_table_size, max_table_size, &info,
                                    HASH_ELEM | HASH_FUNCTION | HASH_PARTITION | HASH_FIXED_SIZE);

    // Step 3: Initialize PredXact list for serializable transactions
    max_table_size = (MaxBackends + max_prepared_xacts) * 10; // 10 transactions per backend

    PredXact = ShmemInitStruct("PredXactList", PredXactListDataSize, &found);

    if (!found) {
        // Initialize available and active transaction lists
        dlist_init(&PredXact->availableList);
        dlist_init(&PredXact->activeList);

        // Initialize global state variables
        PredXact->SxactGlobalXmin = InvalidTransactionId;
        PredXact->WritableSxactCount = 0;
        PredXact->LastSxactCommitSeqNo = FirstNormalSerCommitSeqNo - 1;

        // Allocate and initialize transaction elements
        requestSize = mul_size(max_table_size, sizeof(SERIALIZABLEXACT));
        PredXact->element = ShmemAlloc(requestSize);
        memset(PredXact->element, 0, requestSize);

        for (int i = 0; i < max_table_size; i++) {
            LWLockInitialize(&PredXact->element[i].perXactPredicateListLock,
                           LWTRANCHE_PER_XACT_PREDICATE_LIST);
            dlist_push_tail(&PredXact->availableList, &PredXact->element[i].xactLink);
        }

        // Create special OldCommittedSxact for representing old transactions
        PredXact->OldCommittedSxact = CreatePredXact();
        initialize_old_committed_transaction(PredXact->OldCommittedSxact);
    }

    OldCommittedSxact = PredXact->OldCommittedSxact;

    // Step 4: Initialize SerializableXidHash for per-XID serializable transaction info
    info.keysize = sizeof(SERIALIZABLEXIDTAG);
    info.entrysize = sizeof(SERIALIZABLEXID);

    SerializableXidHash = ShmemInitHash("SERIALIZABLEXID hash",
                                      max_table_size, max_table_size, &info,
                                      HASH_ELEM | HASH_BLOBS | HASH_FIXED_SIZE);

    // Step 5: Initialize RWConflictPool for tracking read-write conflicts
    max_table_size *= 5; // Assume 5 conflicts per transaction

    RWConflictPool = ShmemInitStruct("RWConflictPool", RWConflictPoolHeaderDataSize, &found);

    if (!found) {
        dlist_init(&RWConflictPool->availableList);
        requestSize = mul_size(max_table_size, RWConflictDataSize);
        RWConflictPool->element = ShmemAlloc(requestSize);
        memset(RWConflictPool->element, 0, requestSize);

        for (int i = 0; i < max_table_size; i++) {
            dlist_push_tail(&RWConflictPool->availableList,
                          &RWConflictPool->element[i].outLink);
        }
    }

    // Step 6: Initialize finished serializable transactions list
    FinishedSerializableTransactions = ShmemInitStruct("FinishedSerializableTransactions",
                                                     sizeof(dlist_head), &found);
    if (!found) {
        dlist_init(FinishedSerializableTransactions);
    }

    // Step 7: Initialize SLRU storage for old committed transactions
    SerialInit();
}

// Helper function abstracted from detailed initialization
static void initialize_old_committed_transaction(SERIALIZABLEXACT *sxact) {
    SetInvalidVirtualTransactionId(sxact->vxid);
    sxact->prepareSeqNo = 0;
    sxact->commitSeqNo = 0;
    sxact->topXid = InvalidTransactionId;
    sxact->flags = SXACT_FLAG_COMMITTED;
    sxact->pid = 0;
    sxact->pgprocno = INVALID_PROC_NUMBER;

    // Initialize all conflict and lock lists
    dlist_init(&sxact->outConflicts);
    dlist_init(&sxact->inConflicts);
    dlist_init(&sxact->predicateLocks);
    dlist_init(&sxact->possibleUnsafeConflicts);
}
```

Key simplifications made:
- Removed detailed error handling and assertions for clarity
- Consolidated repetitive hash table initialization patterns
- Abstracted the complex OldCommittedSxact initialization into a helper function
- Simplified the nested loops and memory allocation details
- Added step-by-step comments explaining the main initialization phases
- Focused on the core data structure setup rather than low-level memory management
- Removed platform-specific conditional compilation directives