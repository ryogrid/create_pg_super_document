# tuplestore_skiptuples

## Location
src/backend/utils/sort/tuplestore.c: 1135 - 1205

## Overview
Advances over N tuples in either forward or backward direction without returning any data, providing an optimized way to skip multiple tuples at once.

## Definition
```c
bool tuplestore_skiptuples(Tuplestorestate *state, int64 ntuples, bool forward)
```

## Detailed Description
This function efficiently skips over a specified number of tuples in a tuplestore without retrieving or processing the tuple data. It provides optimized implementations for different tuplestore states, particularly for the in-memory case (TSS_INMEM) where it can perform arithmetic operations on position counters rather than iterating through individual tuples.

For the in-memory case, the function directly manipulates the read pointer's current position and handles boundary conditions such as end-of-file states and tuple deletion boundaries. For other cases (such as when tuples are spilled to disk), it falls back to repeatedly calling `tuplestore_gettuple` and discarding the results.

The function includes careful handling of edge cases like backward reading from EOF position and boundary checking to ensure the skip operation doesn't exceed available tuple ranges.

## Parameters / Member Variables
- `state`: Pointer to the Tuplestorestate structure representing the tuplestore
- `ntuples`: Number of tuples to skip (int64), no-op if <= 0
- `forward`: Boolean indicating direction (true for forward, false for backward)

## Dependencies
- Functions called/Symbols referenced:
  - tuplestore_gettuple
  - pfree (for memory cleanup)
  - CHECK_FOR_INTERRUPTS (for query cancellation)
- Types referenced:
  - TSReadPointer
  - EXEC_FLAG_BACKWARD
  - TSS_INMEM
- Called from (representative examples):
  - PersistHoldablePortal
  - window_gettupleslot
  - WinSetMarkPosition

## Notes and Other Information
- Returns true if the skip operation was successful, false if ran out of tuples
- Includes assertion to ensure backward reading is supported when requested
- Optimized for in-memory case with direct position arithmetic
- Falls back to iterative `tuplestore_gettuple` calls for complex cases
- Properly handles EOF states and tuple deletion boundaries
- Includes interrupt checking during long skip operations to allow query cancellation
- Used primarily in window functions and portal management for efficient tuple navigation