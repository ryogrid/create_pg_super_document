# ReplicationState

## Location
[src/backend/replication/logical/origin.c:101-134](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/origin.c#L101-L134)

## Overview
ReplicationState is a structure that tracks the replay progress of a single remote node in PostgreSQL's logical replication system.

## Definition

```c
typedef struct ReplicationState
{
	/*
	 * Local identifier for the remote node.
	 */
	RepOriginId roident;

	/*
	 * Location of the latest commit from the remote side.
	 */
	XLogRecPtr	remote_lsn;

	/*
	 * Remember the local lsn of the commit record so we can XLogFlush() to it
	 * during a checkpoint so we know the commit record actually is safe on
	 * disk.
	 */
	XLogRecPtr	local_lsn;

	/*
	 * PID of backend that's acquired slot, or 0 if none.
	 */
	int			acquired_by;

	/*
	 * Condition variable that's signaled when acquired_by changes.
	 */
	ConditionVariable origin_cv;

	/*
	 * Lock protecting remote_lsn and local_lsn.
	 */
	LWLock		lock;
} ReplicationState;
```
## Detailed Description
The ReplicationState structure is a core component of PostgreSQL's logical replication origin tracking system. It maintains the replication progress state for a single remote replication origin, including both the remote and local log sequence numbers (LSNs). This structure is crucial for tracking replication lag, ensuring data consistency, and managing concurrent access to replication state information.

The structure includes synchronization primitives (condition variable and lightweight lock) to coordinate access between different backend processes that may need to read or modify the replication state. The acquired_by field tracks which backend process currently holds exclusive access to modify this replication state.

## Parameters / Member Variables
- : Local identifier for the remote replication origin node
- : XLog location of the latest commit received from the remote side
- : Local XLog location of the commit record for flushing during checkpoints to ensure durability
- : Process ID of the backend that has acquired this slot, or 0 if available
- : Condition variable signaled when the acquired_by field changes
- : Lightweight lock protecting access to remote_lsn and local_lsn fields

## Dependencies
- Functions called/Symbols referenced:
  - RepOriginId (replication origin identifier type)
  - ConditionVariable (synchronization primitive)
  - [LWLock](../L/LWLock.md) (lightweight lock type)
- Called from (representative examples):
  - [ReplicationStateCtl](ReplicationStateCtl.md)
  - [replorigin_advance](../r/replorigin_advance.md)
  - [replorigin_get_progress](../r/replorigin_get_progress.md)
  - [replorigin_session_setup](../r/replorigin_session_setup.md)
  - [CheckPointReplicationOrigin](../C/CheckPointReplicationOrigin.md)

## Notes and Other Information
- The structure is allocated in shared memory as part of the replication origin control structure
- The lock field specifically protects the LSN fields to ensure atomic updates during replication progress tracking
- The condition variable mechanism allows efficient waiting for slot availability when multiple processes compete for the same replication origin
- This structure is persistent across server restarts through checkpoint and recovery mechanisms