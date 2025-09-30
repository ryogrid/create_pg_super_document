# AfterTriggerEnlargeQueryState

## Location
[src/backend/commands/trigger.c:5624-5670](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L5624-L5670)

## Overview
AfterTriggerEnlargeQueryState prepares the necessary state to record AFTER trigger events queued by a query, managing separate state for each query nesting level within a transaction.

## Definition

```c
static void
AfterTriggerEnlargeQueryState(void)
```
## Detailed Description
This function ensures that the afterTriggers query stack has sufficient capacity to handle the current query depth. PostgreSQL allows nested queries within a (sub)transaction, and each nesting level requires its own separate trigger state to properly manage AFTER trigger events. The function dynamically grows the query_stack array when needed, either by initial allocation or reallocation, and initializes new entries to empty state.

The function operates on the global afterTriggers structure and ensures that maxquerydepth is at least as large as query_depth + 1. It uses an exponential growth strategy for efficiency, doubling the allocation size when expansion is needed.

## Parameters / Member Variables
This function takes no parameters and operates on global state:
- Uses : Current nesting level of queries
- Uses : Maximum allocated depth in query_stack
- Uses : Array of AfterTriggersQueryData structures

## Dependencies
- Functions called/Symbols referenced:
  - [AfterTriggersQueryData](AfterTriggersQueryData.md) (struct type)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md) (for initial allocation)
  - [repalloc](../r/repalloc.md) (for reallocation)
  - SetConstraintState (referenced but not called directly)
- Called from:
  - [MakeTransitionCaptureState](../M/MakeTransitionCaptureState.md) (src/backend/commands/trigger.c:5020)
  - [AfterTriggerSaveEvent](AfterTriggerSaveEvent.md) (src/backend/commands/trigger.c:6177)  
  - [before_stmt_triggers_fired](../b/before_stmt_triggers_fired.md) (src/backend/commands/trigger.c:6556)

## Notes and Other Information
- Memory allocation occurs in TopTransactionContext to ensure proper cleanup
- New array entries are initialized with NULL/NIL values for all fields
- The function uses exponential growth (doubling) for efficient memory management
- Initial allocation size is at least 8 entries to reduce frequent reallocations
- This is part of PostgreSQL's deferred trigger execution system

## Simplified Source

```c
static void
AfterTriggerEnlargeQueryState(void)
{
    int init_depth = afterTriggers.maxquerydepth;

    Assert(afterTriggers.query_depth >= afterTriggers.maxquerydepth);

    if (afterTriggers.maxquerydepth == 0) {
        // Initial allocation - allocate at least 8 entries
        int new_alloc = Max(afterTriggers.query_depth + 1, 8);

        afterTriggers.query_stack = (AfterTriggersQueryData *)
            MemoryContextAlloc(TopTransactionContext,
                               new_alloc * sizeof(AfterTriggersQueryData));
        afterTriggers.maxquerydepth = new_alloc;
    } else {
        // Grow existing stack using exponential growth strategy
        int old_alloc = afterTriggers.maxquerydepth;
        int new_alloc = Max(afterTriggers.query_depth + 1, old_alloc * 2);

        afterTriggers.query_stack = (AfterTriggersQueryData *)
            repalloc(afterTriggers.query_stack,
                     new_alloc * sizeof(AfterTriggersQueryData));
        afterTriggers.maxquerydepth = new_alloc;
    }

    // Initialize new entries to empty state
    while (init_depth < afterTriggers.maxquerydepth) {
        AfterTriggersQueryData *qs = &afterTriggers.query_stack[init_depth];

        qs->events.head = NULL;
        qs->events.tail = NULL;
        qs->events.tailfree = NULL;
        qs->fdw_tuplestore = NULL;
        qs->tables = NIL;

        ++init_depth;
    }
}
```