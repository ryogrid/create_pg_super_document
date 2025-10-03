# MemoryContextMemAllocated

## Location
[src/backend/utils/mmgr/mcxt.c:762-785](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/mcxt.c#L762-L785)

## Overview
MemoryContextMemAllocated reports the amount of memory allocated in a memory context, with optional recursion to include all child contexts in the calculation.

## Definition
```c
Size MemoryContextMemAllocated(MemoryContext context, bool recurse)
```

## Detailed Description
MemoryContextMemAllocated provides memory usage statistics for a memory context by returning the total amount of allocated memory. The function operates in two modes based on the recurse parameter:

1. **Non-recursive mode (recurse = false)**: Returns only the memory allocated directly in the specified context (context->mem_allocated).

2. **Recursive mode (recurse = true)**: Returns the sum of memory allocated in the specified context plus all its descendant contexts. It traverses the context tree using MemoryContextTraverseNext to visit all child contexts and accumulates their mem_allocated values.

The mem_allocated field tracks the actual memory allocated for user data blocks, excluding memory management overhead. This makes it useful for:
- Memory usage monitoring and profiling
- Hash aggregation memory limit checking
- Resource management and optimization decisions
- Debugging memory consumption patterns

The function is particularly important in PostgreSQL's hash aggregation implementation, where it helps determine when to switch from hash-based to sort-based aggregation due to memory pressure.

## Parameters / Member Variables
- `context`: The memory context whose allocated memory is to be calculated; must be a valid, initialized MemoryContext
- `recurse`: Boolean flag indicating whether to include child contexts in the calculation

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextIsValid (validates the context)
  - [MemoryContextTraverseNext](MemoryContextTraverseNext.md) (traverses child contexts when recursing)
- Called from (representative examples):
  - [hash_agg_check_limits](../h/hash_agg_check_limits.md) (hash aggregation memory management)
  - [hash_agg_update_metrics](../h/hash_agg_update_metrics.md) (hash aggregation metrics tracking)
  - [RT_MEMORY_USAGE](../R/RT_MEMORY_USAGE.md) (radix tree memory usage calculation)

## Notes and Other Information
- Returns the total allocated memory as a Size type (typically size_t)
- The function includes an Assert to validate the input context in debug builds
- When recursing, it performs a depth-first traversal of the entire context subtree
- The mem_allocated field is maintained automatically by the memory context system as allocations and deallocations occur
- This function reports allocated memory, not consumed memory - it doesn't account for freed blocks that might still be retained by the context
- Used extensively in hash aggregation to implement memory usage limits and prevent out-of-memory conditions
- The traversal is efficient and doesn't modify any context state
- Memory usage includes only the payload data, not the memory management overhead structures

## Simplified Source

```c
Size
MemoryContextMemAllocated(MemoryContext context, bool recurse)
{
    Size total = context->mem_allocated;

    // If recursing, add memory from all child contexts
    if (recurse) {
        for (MemoryContext child = context->firstchild;
             child != NULL;
             child = MemoryContextTraverseNext(child, context)) {
            total += child->mem_allocated;
        }
    }

    return total;
}
```