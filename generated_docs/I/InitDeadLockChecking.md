# InitDeadLockChecking

## Location
[src/backend/storage/lmgr/deadlock.c:143-216](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/deadlock.c#L143-L216)

## Overview
Initializes the deadlock checker during backend startup by allocating working memory for deadlock detection algorithms.

## Definition
void InitDeadLockChecking(void)

## Detailed Description
InitDeadLockChecking performs per-backend initialization of the deadlock checker by allocating working memory structures needed for deadlock detection. The function allocates memory in the TopMemoryContext to ensure permanence, as the deadlock checker might be invoked during memory shortage situations or within signal handlers where palloc is dangerous.

The function sets up several key data structures:
- Arrays for tracking visited processes during cycle detection
- Constraint arrays for topological sorting
- Wait order structures for queue rearrangement
- Edge arrays for storing possible and current constraints

All allocations are sized based on MaxBackends to handle the worst-case scenario where all backends might be involved in a deadlock scenario.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [palloc](../p/palloc.md)
  - [PGPROC](../P/PGPROC.md) (struct type)
  - DEADLOCK_INFO (struct type)
  - WAIT_ORDER (struct type)
  - [EDGE](../E/EDGE.md) (struct type)
- Called from (representative examples):
  - [InitProcess](InitProcess.md)
  - LockHashPartitionLockByProc

## Notes and Other Information
- Memory is allocated in TopMemoryContext to ensure it persists for the lifetime of the backend
- The sizing is conservative, allocating space for MaxBackends entries in most arrays
- Some arrays are shared/reused between different phases of deadlock detection (e.g., topoProcs reuses visitedProcs space)
- The maxCurConstraints limit also controls the maximum recursion depth of DeadLockCheckRecurse to prevent stack overflow
- This initialization is done once per backend startup, not per deadlock check, for performance and safety reasons

## Simplified Source

```c
// Simplified version of InitDeadLockChecking
void InitDeadLockChecking(void) {
    MemoryContext oldcxt;

    // Switch to permanent memory context for persistent allocations
    oldcxt = MemoryContextSwitchTo(TopMemoryContext);

    // Allocate arrays for deadlock cycle detection (sized for worst case)
    visitedProcs = palloc(MaxBackends * sizeof(PGPROC *));
    deadlockDetails = palloc(MaxBackends * sizeof(DEADLOCK_INFO));

    // Allocate arrays for topological sorting (reuse visitedProcs space)
    topoProcs = visitedProcs;
    beforeConstraints = palloc(MaxBackends * sizeof(int));
    afterConstraints = palloc(MaxBackends * sizeof(int));

    // Allocate arrays for wait queue rearrangement
    waitOrders = palloc((MaxBackends / 2) * sizeof(WAIT_ORDER));
    waitOrderProcs = palloc(MaxBackends * sizeof(PGPROC *));

    // Allocate constraint tracking arrays
    maxCurConstraints = MaxBackends;
    curConstraints = palloc(maxCurConstraints * sizeof(EDGE));

    maxPossibleConstraints = MaxBackends * 4;
    possibleConstraints = palloc(maxPossibleConstraints * sizeof(EDGE));

    // Restore original memory context
    MemoryContextSwitchTo(oldcxt);
}
```

Key simplifications made:
- Removed detailed explanatory comments for brevity
- Consolidated related allocations with brief descriptions
- Abstracted the rationale into high-level comments
- Focused on the core allocation sequence and memory management pattern