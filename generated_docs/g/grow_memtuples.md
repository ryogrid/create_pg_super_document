# grow_memtuples

## Location
[src/backend/utils/sort/tuplestore.c:578-707](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplestore.c#L578-L707)

## Overview
Attempts to grow the memory tuple array in a tuplesort state within memory constraints, using adaptive sizing strategies to maximize memory utilization.

## Definition

```c
static bool
grow_memtuples(Tuplestorestate *state)
```
## Detailed Description
This function implements a sophisticated memory management strategy for growing the  array in tuplesort operations. It uses two growth strategies:

1. **Doubling Strategy**: When memory usage is <= 50% of allowed memory, it doubles the array size (clamped at INT_MAX)
2. **Proportional Strategy**: When memory usage > 50%, it calculates an optimal size based on current memory utilization ratio

The function includes several safety checks:
- Ensures the array doesn't exceed INT_MAX tuples
- Prevents exceeding the caller-provided memory limit
- Clamps allocation requests to MaxAllocHugeSize on 32-bit systems
- Validates that growth won't trigger LACKMEM condition

Key design principles:
- Uses  flag to prevent repeated failed attempts
- Performs calculations in float8 to avoid integer overflow
- Prefers one substantial increase over multiple small increases
- Balances between array space and tuple storage space

## Parameters / Member Variables
- : Pointer to the  structure containing the sort operation state

## Dependencies
- Functions called/Symbols referenced:
  -  - decrements available memory tracking
  -  - increments available memory tracking  
  -  - checks if memory is exhausted
  -  - gets allocated memory chunk size
  -  - reallocates large memory blocks
  -  - logs error messages
  - Constants: , 
  - Types: , 
- Called from (representative examples):
  -  (tuplesort.c:1244)
  -  (tuplestore.c:798)

## Notes and Other Information
- Returns  if array was successfully enlarged,  otherwise
- Sets  to  when no further growth is possible
- Critical for performance in large sorting operations
- Implements adaptive memory management to maximize tuple capacity
- Handles both 32-bit and 64-bit system constraints
- Used by both tuplesort and tuplestore subsystems
- Memory calculations account for both array overhead and tuple storage needs

## Simplified Source

```c
static bool
grow_memtuples(Tuplesortstate *state)
{
    int newmemtupsize;
    int memtupsize = state->memtupsize;
    int64 memNowUsed = state->allowedMem - state->availMem;

    // Don't attempt if we've already maxed out
    if (!state->growmemtuples)
        return false;

    // Choose growth strategy based on current memory usage
    if (memNowUsed <= state->availMem) {
        // Using <= 50% of memory: double the size (clamped at INT_MAX)
        if (memtupsize < INT_MAX / 2)
            newmemtupsize = memtupsize * 2;
        else {
            newmemtupsize = INT_MAX;
            state->growmemtuples = false;
        }
    } else {
        // Using > 50% of memory: calculate proportional growth
        // This will be the final growth attempt
        double grow_ratio = (double) state->allowedMem / (double) memNowUsed;

        if (memtupsize * grow_ratio < INT_MAX)
            newmemtupsize = (int) (memtupsize * grow_ratio);
        else
            newmemtupsize = INT_MAX;

        state->growmemtuples = false;
    }

    // Must actually grow by at least one element
    if (newmemtupsize <= memtupsize)
        goto noalloc;

    // Clamp to system allocation limits on 32-bit systems
    if ((Size) newmemtupsize >= MaxAllocHugeSize / sizeof(SortTuple)) {
        newmemtupsize = (int) (MaxAllocHugeSize / sizeof(SortTuple));
        state->growmemtuples = false;
    }

    // Ensure growth fits within available memory
    if (state->availMem < (int64) ((newmemtupsize - memtupsize) * sizeof(SortTuple)))
        goto noalloc;

    // Perform the reallocation
    FREEMEM(state, GetMemoryChunkSpace(state->memtuples));
    state->memtupsize = newmemtupsize;
    state->memtuples = (SortTuple *)
        repalloc_huge(state->memtuples, state->memtupsize * sizeof(SortTuple));
    USEMEM(state, GetMemoryChunkSpace(state->memtuples));

    // Verify we didn't trigger out-of-memory condition
    if (LACKMEM(state))
        elog(ERROR, "unexpected out-of-memory situation in tuplesort");

    return true;

noalloc:
    // Failed to allocate - disable future attempts
    state->growmemtuples = false;
    return false;
}
```