# window_gettupleslot

## Location
[src/backend/executor/nodeWindowAgg.c:3066-3169](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeWindowAgg.c#L3066-L3169)

## Overview
Fetches the pos'th tuple of the current partition into the specified slot using the WindowObject's read pointer for window function operations.

## Definition


## Detailed Description
This function is a core utility for window function processing that retrieves a specific tuple from the current partition based on its position. It manages tuple access through a tuplestore buffer system and maintains position tracking through the WindowObject's read pointer. The function handles efficient seeking by positioning the read pointer close to the target position and then making minimal forward or backward movements to reach the exact tuple. It ensures that fetched tuples are physically copied to maintain validity across tuplestore manipulations.

## Parameters / Member Variables
- : WindowObject containing state information including the window aggregate state, read pointer, seek position, and mark position
- : Zero-based position of the tuple to fetch within the current partition (must be >= 0)
- : TupleTableSlot where the fetched tuple will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [spool_tuples](../s/spool_tuples.md)
  - [tuplestore_select_read_pointer](../t/tuplestore_select_read_pointer.md)
  - [tuplestore_skiptuples](../t/tuplestore_skiptuples.md)
  - [tuplestore_advance](../t/tuplestore_advance.md)
  - [tuplestore_gettupleslot](../t/tuplestore_gettupleslot.md)
- Called from (representative examples):
  - [eval_windowaggregates](../e/eval_windowaggregates.md)
  - [WinRowsArePeers](../W/WinRowsArePeers.md)
  - [WinGetFuncArgInPartition](../W/WinGetFuncArgInPartition.md)
  - [WinGetFuncArgInFrame](../W/WinGetFuncArgInFrame.md)

## Notes and Other Information
- Returns true if successful, false if no such row exists
- Validates that the requested position is not before the WindowObject's mark position
- Optimizes access patterns by positioning the read pointer within one tuple of the target
- Handles the special case where the current position equals the target by moving forward then backward
- Uses per-query memory context for tuple operations
- Includes interrupt checking for long-running operations
- Ensures physical tuple copies to maintain data validity across tuplestore manipulations