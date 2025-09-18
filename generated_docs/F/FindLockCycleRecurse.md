# FindLockCycleRecurse

## Location
[src/backend/storage/lmgr/deadlock.c:454-532](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/deadlock.c#L454-L532)

## Overview
FindLockCycleRecurse is a recursive function that performs depth-first search to detect deadlock cycles in PostgreSQL's lock dependency graph, handling both individual processes and lock group members.

## Definition
static bool FindLockCycleRecurse(PGPROC *checkProc, int depth, EDGE *softEdges, int *nSoftEdges)

## Detailed Description
This function is the core recursive component of PostgreSQL's deadlock detection algorithm. It performs a depth-first traversal of the lock dependency graph starting from a given process (checkProc) to detect cycles that indicate deadlocks. The function handles complex scenarios including lock groups where multiple processes can act as a single logical entity for locking purposes.

The algorithm maintains a visited processes array to detect cycles. When it encounters a previously visited process, it checks if this creates a cycle that includes the starting point (indicating a true deadlock) or just a cycle that doesn't involve the original process (which is not a deadlock condition).

The function also handles lock groups by checking both the current process and other members of its lock group for potential waits-for edges, as deadlocks can occur between lock group members even when the group leader is not directly waiting.

## Parameters / Member Variables
- : The PGPROC structure representing the process currently being examined for deadlock cycles
- : Current recursion depth in the deadlock detection traversal, used to track cycle length
- : Output array to store information about "soft" edges in the dependency graph that could be broken
- : Output parameter indicating the number of soft edges found during traversal

## Dependencies
- Functions called/Symbols referenced:
  - [FindLockCycleRecurseMember](FindLockCycleRecurseMember.md)
  - dlist_foreach
  - dlist_container
- Called from (representative examples):
  - FindLockCycle
  - [FindLockCycleRecurseMember](FindLockCycleRecurseMember.md) (recursive calls)

## Notes and Other Information
- Uses global variables visitedProcs and nVisitedProcs to track the current path in the dependency graph
- Sets global variable nDeadlockDetails when a deadlock cycle is detected
- Handles lock group semantics by checking the group leader instead of individual members when appropriate
- The function returns true if a deadlock cycle is detected, false otherwise
- Maximum recursion depth is limited by MaxBackends to prevent infinite recursion
- Critical component of PostgreSQL's automatic deadlock resolution mechanism