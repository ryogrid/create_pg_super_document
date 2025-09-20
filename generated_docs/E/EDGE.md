# EDGE

## Location
[src/backend/storage/lmgr/deadlock.c:53-60](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/deadlock.c#L53-L60)

## Overview
EDGE is a data structure used in PostgreSQL's deadlock detection system to represent a waiting relationship between two lock groups in the wait-for graph.

## Definition

```c
typedef struct
{
	LOCK	   *lock;			/* the lock whose wait queue is described */
	PGPROC	  **procs;			/* array of PGPROC *'s in new wait order */
	int			nProcs;
} WAIT_ORDER;
```
## Detailed Description
The EDGE structure represents a directed edge in the wait-for graph used by PostgreSQL's deadlock detection algorithm. Each edge connects two lock groups, indicating that one group is waiting for a lock held by another group. This data structure is fundamental to the deadlock detection mechanism, as it allows the system to build a graph representation of lock dependencies and identify cycles that indicate deadlocks.

The structure includes workspace fields (pred and link) that are used by the topological sorting algorithm to process the wait-for graph and determine if deadlocks exist and how they can be resolved.

## Parameters / Member Variables
- `waiter`: Pointer to the PGPROC that represents the leader of the waiting lock group
- `holder`: Pointer to the PGPROC that represents the leader of the group being waited for
- `lock`: Pointer to the LOCK object that is the subject of the wait relationship
- `pred`: Integer workspace field used by the topological sort algorithm
- `link`: Integer workspace field used by the topological sort algorithm

## Dependencies
- Functions called/Symbols referenced:
  - [PGPROC](../P/PGPROC.md)
  - LOCK
- Called from (representative examples):
  - DEADLOCK_INFO
  - [InitDeadLockChecking](../I/InitDeadLockChecking.md)
  - [TestConfiguration](../T/TestConfiguration.md)
  - FindLockCycle
  - [FindLockCycleRecurse](../F/FindLockCycleRecurse.md)
  - [FindLockCycleRecurseMember](../F/FindLockCycleRecurseMember.md)
  - [ExpandConstraints](ExpandConstraints.md)
  - [TopoSort](../T/TopoSort.md)

## Notes and Other Information
The EDGE structure is defined in src/backend/storage/lmgr/deadlock.c:53-60 and is primarily used within the deadlock detection subsystem. The pred and link fields are temporary workspace variables used during graph traversal algorithms and do not represent persistent state. This structure is essential for PostgreSQL's ability to detect and resolve deadlock situations in a multi-user environment where concurrent transactions may create circular waiting dependencies.