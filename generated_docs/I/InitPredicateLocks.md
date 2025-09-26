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