# LOCALLOCKTAG

## Location
src/include/storage/lock.h: 408 - 412

## Overview
LOCALLOCKTAG serves as a unique identifier for entries in a backend's local lock hash table, combining a lock identifier with a specific lock mode.

## Definition


## Detailed Description
LOCALLOCKTAG is a key structure used in PostgreSQL's local lock management system. Each backend process maintains a local hash table that tracks locks it has acquired or is interested in, and LOCALLOCKTAG serves as the unique key for entries in this table. The combination of a specific lockable object (identified by LOCKTAG) and a particular lock mode creates a unique identifier, allowing the same object to have multiple entries if held in different lock modes.

This local tracking mechanism enables multiple requests for the same lock to be handled without additional shared memory accesses, improving performance. The structure is essential for the fast-path locking mechanism and helps maintain accurate reference counts for lock acquisitions per ResourceOwner.

## Parameters / Member Variables
- : A LOCKTAG structure that uniquely identifies the lockable object (relation, transaction, etc.)
- : The specific lock mode (LOCKMODE) being requested or held on the identified object

## Dependencies
- Functions called/Symbols referenced:
  - LOCKTAG
  - LOCKMODE
- Called from (representative examples):
  - LockAcquireExtended
  - LockRelease
  - LockHeldByMe
  - LockHasWaiters
  - InitLocks

## Notes and Other Information
LOCALLOCKTAG is used as a hash key in the local lock table, allowing backends to track multiple lock modes on the same object separately. This design supports PostgreSQL's hierarchical locking system where a transaction might hold different types of locks on the same resource. The structure is crucial for the fast-path locking optimization, where frequently used locks can be managed locally without contending for shared memory structures.