# tuplestore_alloc_read_pointer

## Location
[src/backend/utils/sort/tuplestore.c:383-417](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplestore.c#L383-L417)

## Overview
Function to allocate an additional read pointer for a tuplestore, enabling multiple concurrent reading positions within the same tuplestore.

## Definition

```c
int
tuplestore_alloc_read_pointer(Tuplestorestate *state, int eflags)
```
## Detailed Description
This function creates a new read pointer that allows independent positioning within the tuplestore data. Multiple read pointers enable scenarios where different parts of the code need to scan through the tuplestore at different positions simultaneously. The new read pointer initially copies the position of read pointer 0, then can be moved independently.

The function enforces capability constraints to ensure that adding new requirements after data insertion doesn't violate the tuplestore's established execution strategy. If data has already been inserted, the new eflags must not increase the overall requirements beyond what was previously established.

The read pointer array is dynamically resized if necessary, doubling in size when the current capacity is exceeded.

## Parameters / Member Variables
- : Pointer to the Tuplestorestate structure to extend with a new read pointer
- : Execution flags for the new read pointer defining its scanning capabilities

## Dependencies
- Functions called/Symbols referenced:
  - elog (error logging function)
  - ERROR (error level constant)
  - [repalloc](../r/repalloc.md) (memory reallocation function)
  - TSS_INMEM (tuplestore status constant)
- Data structures used:
  - [Tuplestorestate](../T/Tuplestorestate.md) (main tuplestore state structure)
  - TSReadPointer (read pointer structure)
- Called from:
  - [ExecInitCteScan](../E/ExecInitCteScan.md) (CTE scan node initialization)
  - [ExecMaterial](../E/ExecMaterial.md) (material node execution)
  - [ExecInitNamedTuplestoreScan](../E/ExecInitNamedTuplestoreScan.md) (named tuplestore scan initialization)
  - [begin_partition](../b/begin_partition.md) (window aggregation partitioning - multiple calls for different pointers)

## Notes and Other Information
- Returns the index of the newly allocated read pointer
- New pointer initially copies position and state from read pointer 0
- After data insertion, new eflags cannot increase tuplestore requirements
- Read pointer array grows dynamically, doubling in size when needed
- Each read pointer can have independent eflags and position
- Commonly used in window functions that need multiple scanning positions
- The returned index is used in subsequent tuplestore operations to specify which read pointer to use

## Simplified Source

```c
int
tuplestore_alloc_read_pointer(Tuplestorestate *state, int eflags)
{
    // Check if we can still modify requirements
    if (state->status != TSS_INMEM || state->memtupcount != 0) {
        if ((state->eflags | eflags) != state->eflags) {
            elog(ERROR, "too late to require new tuplestore eflags");
        }
    }

    // Expand read pointer array if needed
    if (state->readptrcount >= state->readptrsize) {
        int newsize = state->readptrsize * 2;
        state->readptrs = repalloc(state->readptrs, newsize * sizeof(TSReadPointer));
        state->readptrsize = newsize;
    }

    // Initialize new read pointer by copying read pointer 0
    state->readptrs[state->readptrcount] = state->readptrs[0];
    state->readptrs[state->readptrcount].eflags = eflags;

    // Update global flags and return new pointer index
    state->eflags |= eflags;
    return state->readptrcount++;
}
```