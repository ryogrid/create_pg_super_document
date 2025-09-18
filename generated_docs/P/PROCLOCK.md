# PROCLOCK

## Location
[src/include/storage/lock.h:369-380](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/lock.h#L369-L380)

## Overview
PROCLOCK represents the association between a process (PGPROC) and a lock object, tracking which lock types a process holds or is waiting for on a specific resource.

## Definition


## Detailed Description
PROCLOCK is a crucial data structure in PostgreSQL's lock management system that maintains the relationship between processes and locks. Each PROCLOCK instance represents a specific process's interest in a particular lock object, whether that interest is an already-granted lock or a pending lock request. The structure serves as a bidirectional link, allowing efficient traversal from locks to processes and vice versa through embedded doubly-linked list nodes.

The PROCLOCK structure is stored in a shared memory hash table and enables the lock manager to track which processes hold which types of locks on specific resources, manage lock conflicts, detect deadlocks, and handle lock release during transaction commit or process termination.

## Parameters / Member Variables
- : A PROCLOCKTAG that uniquely identifies this proclock object, combining lock and process identifiers
- : Pointer to the process that leads the lock group, or points to the process itself if not part of a group
- : Bitmask indicating which lock types this process currently holds on the associated resource
- : Bitmask indicating which lock types should be released (used during transaction commit/abort)
- : Doubly-linked list node for chaining this proclock in the associated LOCK's list of proclocks
- : Doubly-linked list node for chaining this proclock in the associated PGPROC's list of proclocks

## Dependencies
- Functions called/Symbols referenced:
  - PROCLOCKTAG
  - [PGPROC](PGPROC.md)
  - LOCKMASK
  - [dlist_node](../d/dlist_node.md)
- Called from (representative examples):
  - [LockAcquireExtended](../L/LockAcquireExtended.md)
  - [LockRelease](../L/LockRelease.md)
  - [SetupLockInTable](../S/SetupLockInTable.md)
  - [LockCheckConflicts](../L/LockCheckConflicts.md)
  - [FastPathTransferRelationLocks](../F/FastPathTransferRelationLocks.md)
  - [GetLockStatusData](../G/GetLockStatusData.md)

## Notes and Other Information
PROCLOCK objects are managed in a hash table indexed by PROCLOCKTAG, which combines the lock identifier and process identifier. The structure enables efficient lock management operations such as deadlock detection, lock release during process cleanup, and fast-path lock optimizations. The bidirectional linking through lockLink and procLink allows the system to efficiently traverse from locks to their holders and from processes to their held locks.