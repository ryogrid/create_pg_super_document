# mergeruns

## Location
[src/backend/utils/sort/tuplesort.c:2045-2231](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesort.c#L2045-L2231)

## Overview
Implements the balanced k-way merge algorithm to merge all completed initial runs into a final sorted result, handling both single-pass and multi-pass external sorting scenarios.

## Definition

```c
enum;
```
## Detailed Description
The  function is the core implementation of PostgreSQL's external merge sort algorithm. It takes multiple sorted runs that have been written to tape and merges them into progressively fewer, longer runs until a single sorted result remains.

**Key Operations:**
1. **Abbreviation Management**: Disables abbreviated key comparisons since abbreviated keys aren't stored on disk
2. **Memory Reorganization**: Frees the large memtuples array and resets tuple memory context to prepare for merge operations
3. **Slab Allocator Setup**: Initializes efficient fixed-size allocation for tuple headers during merge
4. **Buffer Management**: Redistributes available memory among input and output tape buffers for optimal I/O performance
5. **Multi-Pass Merge**: Executes merge passes until only one run remains, converting outputs from one pass into inputs for the next

**Optimization Features:**
- **Final Merge Optimization**: If conditions allow (no random access needed, single run per tape), performs the final merge on-the-fly without writing to tape
- **Memory Distribution**: Dynamically allocates tape buffer memory based on the number of input/output tapes
- **Worker Support**: Handles both standalone and parallel worker contexts

The algorithm continues until all input runs are consumed and only one output run exists, representing the complete sorted dataset.

## Parameters / Member Variables
- : Pointer to the  structure containing:
  - : Must be  when called
  - : Should be 0 (all tuples written to tape)
  - /: Number of input/output tapes
  - /: Number of runs on input/output tapes
  - : Collection of logical tapes
  - : Available memory for tape buffers
  - : Final tape containing sorted result

## Dependencies
- Functions called/Symbols referenced:
  - : Sets up efficient tuple allocation
  - : Chooses next output tape for runs
  - : Merges one run from each input tape
  - : Initializes final merge state
  - /: Tape management
  - : Finalizes result tape
  - : Calculates optimal buffer sizes
  - /: Memory management
  - /: Memory usage tracking

- Called from (representative examples):
  - : Main sorting entry point
  - : When initiating external sort

## Notes and Other Information
- This is a static function within tuplesort.c, internal to the sorting implementation
- Implements the balanced k-way merge which is optimal for external sorting
- Handles both single-pass and multi-pass merging depending on available memory and run count
- Critical performance optimizations include final merge on-the-fly and dynamic buffer allocation
- Supports both standalone and parallel worker execution contexts
- The function changes the sort state from  to either  or 
- Memory management is sophisticated, transitioning from tuple-based to slab-based allocation
- Part of PostgreSQL's highly optimized external sorting system for handling large datasets

## Simplified Source

```c
static void
mergeruns(Tuplesortstate *state)
{
    int tapenum;

    Assert(state->status == TSS_BUILDRUNS);
    Assert(state->memtupcount == 0);

    // Disable abbreviation keys for merge phase (not stored on disk)
    if (state->base.sortKeys != NULL && state->base.sortKeys->abbrev_converter != NULL) {
        state->base.sortKeys->abbrev_converter = NULL;
        state->base.sortKeys->comparator = state->base.sortKeys->abbrev_full_comparator;
        state->base.sortKeys->abbrev_abort = NULL;
        state->base.sortKeys->abbrev_full_comparator = NULL;
    }

    // Reset memory for merge phase
    MemoryContextResetOnly(state->base.tuplecontext);

    // Free large memtuples array, allocate smaller one for heap
    pfree(state->memtuples);
    state->memtuples = NULL;

    // Initialize slab allocator for efficient tuple allocation
    if (state->base.tuples)
        init_slab_allocator(state, state->nOutputTapes + 1);
    else
        init_slab_allocator(state, 0);

    // Allocate new memtuples array for merge heap
    state->memtupsize = state->nOutputTapes;
    state->memtuples = (SortTuple *) MemoryContextAlloc(state->base.maincontext,
                                                        state->nOutputTapes * sizeof(SortTuple));

    // Allocate remaining memory for tape buffers
    state->tape_buffer_mem = state->availMem;

    // Main merge loop
    for (;;) {
        // Start new merge pass if no input runs remain
        if (state->nInputRuns == 0) {
            // Close empty input tapes
            if (state->nInputTapes > 0) {
                for (tapenum = 0; tapenum < state->nInputTapes; tapenum++)
                    LogicalTapeClose(state->inputTapes[tapenum]);
                pfree(state->inputTapes);
            }

            // Previous outputs become next pass inputs
            state->inputTapes = state->outputTapes;
            state->nInputTapes = state->nOutputTapes;
            state->nInputRuns = state->nOutputRuns;

            // Reset output tape variables
            state->outputTapes = palloc0(state->nInputTapes * sizeof(LogicalTape *));
            state->nOutputTapes = 0;
            state->nOutputRuns = 0;

            // Calculate buffer sizes and prepare input tapes
            input_buffer_size = merge_read_buffer_size(state->tape_buffer_mem,
                                                     state->nInputTapes,
                                                     state->nInputRuns,
                                                     state->maxTapes);

            for (tapenum = 0; tapenum < state->nInputTapes; tapenum++)
                LogicalTapeRewindForRead(state->inputTapes[tapenum], input_buffer_size);

            // Check if we can do final merge on-the-fly
            if ((state->base.sortopt & TUPLESORT_RANDOMACCESS) == 0 &&
                state->nInputRuns <= state->nInputTapes &&
                !WORKER(state)) {
                LogicalTapeSetForgetFreeSpace(state->tapeset);
                beginmerge(state);
                state->status = TSS_FINALMERGE;
                return;
            }
        }

        // Select output tape and merge one run from each input
        selectnewtape(state);
        mergeonerun(state);

        // Check if merge is complete
        if (state->nInputRuns == 0 && state->nOutputRuns <= 1)
            break;
    }

    // Finalize result - single run on single tape
    state->result_tape = state->outputTapes[0];
    if (!WORKER(state))
        LogicalTapeFreeze(state->result_tape, NULL);
    else
        worker_freeze_result_tape(state);
    state->status = TSS_SORTEDONTAPE;

    // Close all input tapes to release buffers
    for (tapenum = 0; tapenum < state->nInputTapes; tapenum++)
        LogicalTapeClose(state->inputTapes[tapenum]);
}
```