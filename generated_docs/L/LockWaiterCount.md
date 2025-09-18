# LockWaiterCount

## Location
src/backend/storage/lmgr/lock.c: 4671 - 4700

## Overview
LockWaiterCount is a function that determines the number of lock requesters waiting on a specific lock identified by a LOCKTAG, providing visibility into lock contention for monitoring and debugging purposes.

## Definition


## Detailed Description
This function searches the PostgreSQL lock manager's hash table to find a lock matching the provided LOCKTAG and returns the count of processes that have requested this lock. It performs thread-safe access to the lock hash table by acquiring the appropriate partition lock before searching. The function validates the lock method ID from the locktag and uses hash-based lookup for efficient lock discovery. If the lock is found, it returns the  field from the LOCK structure, which represents the total number of lock requests (both granted and waiting) for that particular lock. If no lock is found for the given locktag, it returns 0.

## Parameters / Member Variables
- : A constant pointer to a LOCKTAG structure that uniquely identifies the lock resource being queried. The LOCKTAG contains the lock method ID, database OID, relation OID, and other identifying information.

## Dependencies
- Functions called/Symbols referenced:
  - lengthof (macro for array length)
  - LockTagHashCode (computes hash value for the lock tag)
  - LockHashPartitionLock (gets the appropriate partition lock)
  - LWLockAcquire (acquires lightweight lock for thread safety)
  - hash_search_with_hash_value (searches the lock hash table)
  - LWLockRelease (releases lightweight lock)
- Called from (representative examples):
  - RelationExtensionLockWaiterCount (wrapper for relation extension locks)
  - LockHashPartitionLockByProc (referenced in header for related functionality)

## Notes and Other Information
- The function uses PostgreSQL's partitioned hash table approach for scalable lock management
- Thread-safe implementation using LWLock to protect hash table access
- Returns the total number of requesters (nRequested), not just waiters - this includes both granted and waiting lock requests
- Error handling validates the lock method ID to prevent invalid memory access
- The function is primarily used for monitoring lock contention and debugging deadlock situations
- Part of PostgreSQL's sophisticated lock manager system that handles various lock types including relation locks, tuple locks, and advisory locks