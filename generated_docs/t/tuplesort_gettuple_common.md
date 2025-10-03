# tuplesort_gettuple_common

## Location
[src/backend/utils/sort/tuplesort.c:1496-1735](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesort.c#L1496-L1735)

## Overview
The core internal function that fetches the next tuple in either forward or backward direction during the tuple sorting process, handling different sorting states and memory management strategies.

## Definition

```c
bool
tuplesort_gettuple_common(Tuplesortstate *state, bool forward,
						  SortTuple *stup)
```
## Detailed Description
This is the central tuple retrieval function in PostgreSQL's tuplesort implementation that abstracts the complexity of fetching tuples from different storage contexts. The function handles three distinct sorting states:

1. **TSS_SORTEDINMEM**: When all tuples fit in memory and are sorted in-place in the memtuples array
2. **TSS_SORTEDONTAPE**: When tuples are stored on a single logical tape after being sorted
3. **TSS_FINALMERGE**: During the final merge phase when multiple sorted runs are being merged

The function implements bidirectional tuple access for random access sorts and manages memory through a slab allocator system. It handles EOF conditions, bounded sorts validation, and complex tape positioning for backward scans. The returned tuple belongs to the tuplesort memory context and may be recycled on subsequent calls.

## Parameters / Member Variables
- : The Tuplesortstate containing all sort context including current position, memory management, and tape references
- : Boolean indicating scan direction - true for forward, false for backward (requires TUPLESORT_RANDOMACCESS)
- : Output parameter where the retrieved SortTuple is stored

## Dependencies
- Functions called/Symbols referenced:
  - WORKER (macro to check worker process state)
  - RELEASE_SLAB_SLOT (memory management for slab allocator)
  - [getlen](../g/getlen.md) (reads tuple length from logical tape)
  - READTUP (reads tuple data from tape)
  - [LogicalTapeBackspace](../L/LogicalTapeBackspace.md) (positions tape backward)
  - [LogicalTapeClose](../L/LogicalTapeClose.md) (closes logical tape)
  - [mergereadnext](../m/mergereadnext.md) (reads next tuple during merge)
  - [tuplesort_heap_delete_top](tuplesort_heap_delete_top.md) (heap management during merge)
  - [tuplesort_heap_replace_top](tuplesort_heap_replace_top.md) (heap management during merge)
- Called from (representative examples):
  - [tuplesort_skiptuples](tuplesort_skiptuples.md)
  - [tuplesort_gettupleslot](tuplesort_gettupleslot.md)
  - [tuplesort_getheaptuple](tuplesort_getheaptuple.md)
  - [tuplesort_getindextuple](tuplesort_getindextuple.md)
  - [tuplesort_getbrintuple](tuplesort_getbrintuple.md)
  - [tuplesort_getdatum](tuplesort_getdatum.md)

## Notes and Other Information
- The function enforces that backward scanning requires TUPLESORT_RANDOMACCESS option
- Memory from returned tuples may be recycled on subsequent calls, requiring careful handling by callers
- Backward scanning on tapes involves complex positioning logic to read tuple length headers
- The function validates bounded sort limits to prevent over-fetching
- During final merge, it maintains a heap of the current front tuples from each input run
- The slab allocator is used for memory management when tuples don't fit entirely in memory

## Simplified Source

```c
bool
tuplesort_gettuple_common(Tuplesortstate *state, bool forward, SortTuple *stup)
{
    Assert(!WORKER(state));

    switch (state->status) {
        case TSS_SORTEDINMEM:
            // Tuples sorted in memory array
            if (forward) {
                if (state->current < state->memtupcount) {
                    *stup = state->memtuples[state->current++];
                    return true;
                }
                state->eof_reached = true;
                return false;
            } else {
                // Backward scan
                if (state->current <= 0)
                    return false;

                if (state->eof_reached)
                    state->eof_reached = false;
                else {
                    state->current--;
                    if (state->current <= 0)
                        return false;
                }
                *stup = state->memtuples[state->current - 1];
                return true;
            }

        case TSS_SORTEDONTAPE:
            // Tuples stored on single tape
            // Release previous tuple memory
            if (state->lastReturnedTuple) {
                RELEASE_SLAB_SLOT(state, state->lastReturnedTuple);
                state->lastReturnedTuple = NULL;
            }

            if (forward) {
                if (state->eof_reached)
                    return false;

                unsigned int tuplen = getlen(state->result_tape, true);
                if (tuplen != 0) {
                    READTUP(state, stup, state->result_tape, tuplen);
                    state->lastReturnedTuple = stup->tuple;
                    return true;
                } else {
                    state->eof_reached = true;
                    return false;
                }
            } else {
                // Complex backward tape positioning logic
                if (state->eof_reached) {
                    size_t nmoved = LogicalTapeBackspace(state->result_tape,
                                                       2 * sizeof(unsigned int));
                    if (nmoved == 0)
                        return false;
                    state->eof_reached = false;
                } else {
                    // Navigate to previous tuple
                    size_t nmoved = LogicalTapeBackspace(state->result_tape,
                                                       sizeof(unsigned int));
                    if (nmoved == 0)
                        return false;

                    unsigned int tuplen = getlen(state->result_tape, false);
                    nmoved = LogicalTapeBackspace(state->result_tape,
                                                tuplen + 2 * sizeof(unsigned int));
                    if (nmoved == tuplen + sizeof(unsigned int))
                        return false;  // At beginning of file
                }

                unsigned int tuplen = getlen(state->result_tape, false);
                LogicalTapeBackspace(state->result_tape, tuplen);
                READTUP(state, stup, state->result_tape, tuplen);
                state->lastReturnedTuple = stup->tuple;
                return true;
            }

        case TSS_FINALMERGE:
            // Final merge from multiple runs
            if (state->lastReturnedTuple) {
                RELEASE_SLAB_SLOT(state, state->lastReturnedTuple);
                state->lastReturnedTuple = NULL;
            }

            if (state->memtupcount > 0) {
                int srcTapeIndex = state->memtuples[0].srctape;
                LogicalTape *srcTape = state->inputTapes[srcTapeIndex];
                SortTuple newtup;

                *stup = state->memtuples[0];
                state->lastReturnedTuple = stup->tuple;

                // Get next tuple from same tape and maintain heap
                if (!mergereadnext(state, srcTape, &newtup)) {
                    // Tape exhausted, remove from heap
                    tuplesort_heap_delete_top(state);
                    state->nInputRuns--;
                    LogicalTapeClose(srcTape);
                } else {
                    newtup.srctape = srcTapeIndex;
                    tuplesort_heap_replace_top(state, &newtup);
                }
                return true;
            }
            return false;

        default:
            elog(ERROR, "invalid tuplesort state");
            return false;
    }
}
```