# CreateLocalPredicateLockHash

## Location
src/backend/storage/lmgr/predicate.c: 1930 - 1948

## Overview
Initializes a backend-local hash table to track predicate locks held by the current transaction, supporting PostgreSQL's Serializable Snapshot Isolation implementation.

## Definition
```c
static void CreateLocalPredicateLockHash(void)
```

## Detailed Description
This function creates and initializes the LocalPredicateLockHash, a process-local hash table that tracks the predicate locks acquired by the current serializable transaction. The hash table is essential for efficient predicate lock management, allowing quick lookup and management of locks without requiring global coordination for every operation.

The hash table maps from PREDICATELOCKTARGETTAG (which identifies the lockable object) to LOCALPREDICATELOCK structures (which contain local information about the lock). The table size is limited by the max_predicate_locks_per_xact configuration parameter, ensuring bounded memory usage per transaction.

This local hash table works in conjunction with the global predicate lock management system, providing a fast way to determine which locks a transaction already holds before attempting to acquire new ones or when releasing locks during transaction cleanup.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - hash_create (PostgreSQL's hash table creation function)
  - HASHCTL (hash table control structure)
  - PREDICATELOCKTARGETTAG (key type for predicate lock targets)
  - LOCALPREDICATELOCK (entry type for local lock information)
  - HASH_ELEM, HASH_BLOBS (hash table configuration flags)
- Called from (representative examples):
  - GetSerializableTransactionSnapshotInt (during serializable transaction setup)
  - AttachSerializableXact (when attaching to existing serializable transaction)

## Notes and Other Information
- Static function - only used internally within predicate.c
- Must only be called once per transaction - includes assertion to ensure LocalPredicateLockHash is NULL before creation
- The hash table size is bounded by max_predicate_locks_per_xact configuration parameter
- Uses HASH_BLOBS flag indicating that keys should be compared using memcmp rather than a custom comparison function
- Part of PostgreSQL's Serializable Snapshot Isolation (SSI) implementation
- The local hash table is destroyed when the transaction ends, either through normal commit/abort or process termination
- Essential for performance as it avoids the need to scan global structures for every predicate lock operation