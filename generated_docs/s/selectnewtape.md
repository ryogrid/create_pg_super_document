# selectnewtape

## Location
src/backend/utils/sort/tuplesort.c: 1976 - 2008

## Overview
Selects the next tape to output to during sorting operations, managing tape allocation for both initial run creation and merge passes.

## Definition


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