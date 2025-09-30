# selectnewtape

## Location
[src/backend/utils/sort/tuplesort.c:1976-2008](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesort.c#L1976-L2008)

## Overview
Selects the next tape to output to during sorting operations, managing tape allocation for both initial run creation and merge passes.

## Definition

```c
static void
selectnewtape(Tuplesortstate *state)
```
## Detailed Description
The  function implements the logic for choosing which logical tape should receive the next run of sorted data. It operates in two distinct modes:

1. **Tape Creation Phase**: When the number of output tapes is less than , the function creates new logical tapes using . Each new tape is assigned to hold the next run, and both  and  counters are incremented.

2. **Round-Robin Assignment Phase**: Once the maximum number of tapes is reached, new runs are assigned to existing tapes using a round-robin strategy. The destination tape is selected using the formula , ensuring even distribution of runs across available tapes.

This function is crucial for both the initial run generation phase (when sorting data that doesn't fit in memory) and subsequent merge passes where runs from multiple tapes are merged into fewer tapes.

## Parameters / Member Variables
- : Pointer to the  structure containing:
  - : Current number of active output tapes
  - : Total number of runs created so far
  - : Maximum allowed number of tapes
  - : The currently selected destination tape
  - : Array of pointers to logical tape objects
  - : The tape set containing all logical tapes

## Dependencies
- Functions called/Symbols referenced:
  - : Creates a new logical tape within the tape set
  - : Main sorting state structure

- Called from (representative examples):
  - : During initial tape setup
  - : When starting merge operations
  - : When writing sorted runs to tapes

## Notes and Other Information
- This is a static function within tuplesort.c, internal to the sorting implementation
- The function includes assertions to validate state consistency during tape creation
- Uses a round-robin strategy to ensure balanced distribution of runs across tapes
- Critical for managing the polyphase merge sort algorithm used by PostgreSQL
- The tape selection strategy affects the efficiency of subsequent merge operations
- Works with the logical tape abstraction, which handles the underlying temporary file management

## Simplified Source

```c
static void selectnewtape(Tuplesortstate *state)
{
    // Create new tapes until we reach the maximum limit
    if (state->nOutputTapes < state->maxTapes) {
        // Create a new logical tape
        state->destTape = LogicalTapeCreate(state->tapeset);
        state->outputTapes[state->nOutputTapes] = state->destTape;
        state->nOutputTapes++;
        state->nOutputRuns++;
    } else {
        // Use round-robin assignment to existing tapes
        int tape_index = state->nOutputRuns % state->nOutputTapes;
        state->destTape = state->outputTapes[tape_index];
        state->nOutputRuns++;
    }
}
```