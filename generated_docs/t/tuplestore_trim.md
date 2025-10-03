# tuplestore_trim

## Location
[src/backend/utils/sort/tuplestore.c:1360-1454](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplestore.c#L1360-L1454)

## Overview
Removes no-longer-needed tuples from the tuplestore by freeing memory for tuples that are before the oldest active read pointer, optimizing memory usage while respecting read pointer capabilities.

## Definition
```c
void tuplestore_trim(Tuplestorestate *state)
```

## Detailed Description
This function performs garbage collection on a tuplestore by identifying and removing tuples that are no longer accessible by any read pointer. It finds the oldest position among all active read pointers and removes tuples before that position, with special considerations for efficiency and safety. The function only operates on in-memory tuplestores and respects REWIND capability requirements. It includes optimizations to avoid expensive array operations when the number of removed tuples is small, and handles the common case where only one tuple remains after trimming.

## Parameters / Member Variables
- `state`: The tuplestore state to trim tuples from

## Dependencies
- Functions called/Symbols referenced:
  - [Tuplestorestate](../T/Tuplestorestate.md)
  - EXEC_FLAG_REWIND (execution flag constant)
  - TSS_INMEM (tuplestore status constant)
  - [GetMemoryChunkSpace](../G/GetMemoryChunkSpace.md)
  - FREEMEM (memory management macro)
- Called from (representative examples):
  - [ExecMaterialMarkPos](../E/ExecMaterialMarkPos.md)
  - [ExecWindowAgg](../E/ExecWindowAgg.md)

## Notes and Other Information
- Trimming is disabled if any read pointer requires REWIND capability
- Only operates on in-memory tuplestores (TSS_INMEM state) as file-based trimming is not cost-effective
- Keeps one extra tuple before the oldest current position to accommodate callers who may still reference the previously returned tuple
- Implements performance optimization: avoids array compaction if fewer than 1/8th of tuples are being removed to prevent O(N²) behavior
- Special optimization for the common case where exactly one tuple remains after trimming
- Marks the tuplestore as truncated for assertion checking purposes
- Updates all read pointer positions after array compaction to maintain consistency

## Simplified Source

```c
void
tuplestore_trim(Tuplestorestate *state)
{
    int oldest, nremove, i;

    // Skip if rewind capability required or not in-memory
    if (state->eflags & EXEC_FLAG_REWIND)
        return;
    if (state->status != TSS_INMEM)
        return;

    // Find oldest read pointer position
    oldest = state->memtupcount;
    for (i = 0; i < state->readptrcount; i++) {
        if (!state->readptrs[i].eof_reached)
            oldest = Min(oldest, state->readptrs[i].current);
    }

    // Keep one extra tuple before oldest current position
    nremove = oldest - 1;
    if (nremove <= 0)
        return;

    // Free no-longer-needed tuples
    for (i = state->memtupdeleted; i < nremove; i++) {
        FREEMEM(state, GetMemoryChunkSpace(state->memtuples[i]));
        pfree(state->memtuples[i]);
        state->memtuples[i] = NULL;
    }
    state->memtupdeleted = nremove;
    state->truncated = true;

    // Skip array compaction if less than 1/8th being removed (O(N²) prevention)
    if (nremove < state->memtupcount / 8)
        return;

    // Compact array: optimize for single remaining tuple case
    if (nremove + 1 == state->memtupcount)
        state->memtuples[0] = state->memtuples[nremove];
    else
        memmove(state->memtuples, state->memtuples + nremove,
                (state->memtupcount - nremove) * sizeof(void *));

    // Update state and read pointer positions
    state->memtupdeleted = 0;
    state->memtupcount -= nremove;
    for (i = 0; i < state->readptrcount; i++) {
        if (!state->readptrs[i].eof_reached)
            state->readptrs[i].current -= nremove;
    }
}
```