# MemoryContextMemConsumed

## Location
[src/backend/utils/mmgr/mcxt.c:786-813](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/mcxt.c#L786-L813)

## Overview
MemoryContextMemConsumed collects comprehensive memory consumption statistics for a memory context and all its children, providing detailed memory usage information through a MemoryContextCounters structure.

## Definition
```c
void MemoryContextMemConsumed(MemoryContext context, 
                             MemoryContextCounters *consumed)
```

## Detailed Description
MemoryContextMemConsumed gathers detailed memory consumption statistics for a memory context tree rooted at the specified context. Unlike MemoryContextMemAllocated which returns only a simple size value, this function provides comprehensive metrics through the MemoryContextCounters structure.

The function operates in two phases:

1. **Context Analysis**: It first calls the type-specific stats method on the root context itself to gather its memory usage statistics.

2. **Child Traversal**: It then iterates through all child contexts using MemoryContextTraverseNext, calling each child's stats method to accumulate comprehensive usage data across the entire context subtree.

The MemoryContextCounters structure typically includes metrics such as:
- Total allocated memory
- Total consumed memory (including overhead)
- Number of blocks
- Block overhead information
- Type-specific statistics

This function is primarily used by PostgreSQL's EXPLAIN system to provide detailed memory usage information in query execution plans, helping developers and database administrators understand memory consumption patterns.

## Parameters / Member Variables
- `context`: The root memory context whose consumption statistics are to be gathered; must be a valid, initialized MemoryContext
- `consumed`: Pointer to a MemoryContextCounters structure that will be filled with the accumulated memory consumption statistics

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextIsValid (validates the context)
  - [MemoryContextTraverseNext](MemoryContextTraverseNext.md) (traverses child contexts)
  - [MemoryContextCounters](MemoryContextCounters.md) (statistics structure)
  - context->methods->stats (type-specific statistics gathering)
- Called from (representative examples):
  - [standard_ExplainOneQuery](../s/standard_ExplainOneQuery.md) (EXPLAIN command implementation)
  - [ExplainExecuteQuery](../E/ExplainExecuteQuery.md) (prepared statement execution with EXPLAIN)

## Notes and Other Information
- The function returns void and fills the provided MemoryContextCounters structure with results
- The consumed structure is initialized with memset to ensure clean starting values
- Uses iterative traversal instead of recursion for better performance and stack safety
- The function includes an Assert to validate the input context in debug builds
- Each context type provides its own stats method implementation, allowing for type-specific memory accounting
- The stats method is called with accumulate=false to add statistics to the consumed counters
- This is a read-only operation that doesn't modify any context state
- Commonly used in query execution analysis and performance monitoring tools
- Provides more detailed information than simple allocation tracking, including memory overhead and fragmentation data

## Simplified Source

```c
void MemoryContextMemConsumed(MemoryContext context,
                             MemoryContextCounters *consumed) {
    // Initialize counters to zero
    memset(consumed, 0, sizeof(*consumed));

    // Collect stats from the root context
    context->methods->stats(context, NULL, NULL, consumed, false);

    // Traverse all child contexts and accumulate their stats
    for (MemoryContext child = context->firstchild;
         child != NULL;
         child = MemoryContextTraverseNext(child, context)) {
        child->methods->stats(child, NULL, NULL, consumed, false);
    }
}
```