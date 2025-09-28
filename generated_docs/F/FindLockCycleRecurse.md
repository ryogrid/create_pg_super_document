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

## Simplified Source

```c
// Simplified version of FindLockCycleRecurse
static bool FindLockCycleRecurse(PGPROC *checkProc, int depth,
                                EDGE *softEdges, int *nSoftEdges) {
    int i;
    dlist_iter iter;

    // Step 1: Handle lock group leadership
    // If this process is a lock group member, check the leader instead
    if (checkProc->lockGroupLeader != NULL) {
        checkProc = checkProc->lockGroupLeader;
    }

    // Step 2: Check if we've already visited this process (cycle detection)
    for (i = 0; i < nVisitedProcs; i++) {
        if (visitedProcs[i] == checkProc) {
            // Found a cycle - check if it includes the starting point
            if (i == 0) {
                // True deadlock: cycle includes the starting process
                nDeadlockDetails = depth;  // Record cycle length
                return true;
            }
            // False cycle: doesn't include starting point, not a deadlock
            return false;
        }
    }

    // Step 3: Mark this process as visited
    visitedProcs[nVisitedProcs++] = checkProc;

    // Step 4: Check direct wait-for edges from this process
    if (checkProc->links.next != NULL && checkProc->waitLock != NULL) {
        // Process is waiting - check what it's waiting for
        if (FindLockCycleRecurseMember(checkProc, checkProc, depth,
                                     softEdges, nSoftEdges)) {
            return true;  // Found deadlock through direct wait
        }
    }

    // Step 5: Check wait-for edges from lock group members
    // Even if this process isn't waiting, other group members might be
    dlist_foreach(iter, &checkProc->lockGroupMembers) {
        PGPROC *memberProc = dlist_container(PGPROC, lockGroupLink, iter.cur);

        // Check if group member is waiting (and it's not the current process)
        if (memberProc->links.next != NULL &&
            memberProc->waitLock != NULL &&
            memberProc != checkProc) {

            // Recursively check this group member's dependencies
            if (FindLockCycleRecurseMember(memberProc, checkProc, depth,
                                         softEdges, nSoftEdges)) {
                return true;  // Found deadlock through group member
            }
        }
    }

    // Step 6: No deadlock found in this path
    return false;
}
```

Key simplifications made:
- Added clear step-by-step comments explaining the algorithm phases
- Simplified variable declarations and combined where appropriate
- Clarified the two types of cycles (true deadlock vs. false cycle)
- Made the lock group handling logic more explicit
- Reduced nested conditions for better readability
- Preserved all essential deadlock detection logic
- Focused on the main algorithm flow rather than low-level details