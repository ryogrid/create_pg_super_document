# WinGetFuncArgInPartition

## Location
[src/backend/executor/nodeWindowAgg.c:3310-3397](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeWindowAgg.c#L3310-L3397)

## Overview
Evaluates a window function's argument expression on a specified row within the partition using flexible positioning semantics similar to lseek(2) system call.

## Definition

```c
Datum
WinGetFuncArgInPartition(WindowObject winobj, int argno,
						 int relpos, int seektype, bool set_mark,
						 bool *isnull, bool *isout)
```
## Detailed Description
This function provides flexible row positioning and argument evaluation capabilities for window functions. It can locate rows relative to the current position, partition head, or partition tail, then evaluate a specified argument expression on that row.

The function operates by:
1. Validating the window object and extracting necessary state information
2. Computing the absolute position based on the seek type and relative offset:
   - WINDOW_SEEK_CURRENT: Position relative to current row
   - WINDOW_SEEK_HEAD: Position relative to partition start (0-based)
   - WINDOW_SEEK_TAIL: Position relative to partition end (requires spooling all tuples)
3. Attempting to fetch the tuple at the computed position
4. If successful, optionally setting the mark position and evaluating the argument expression
5. Returning appropriate null values and status flags for out-of-bounds positions

This function is essential for implementing window functions like LAG/LEAD that need to access argument values from rows at specific offsets within the partition.

## Parameters / Member Variables
- : Window object containing partition data and state
- : Zero-based index of the argument expression to evaluate
- : Signed offset from the seek position
- : Position reference point (WINDOW_SEEK_CURRENT, WINDOW_SEEK_HEAD, or WINDOW_SEEK_TAIL)
- : Whether to move the mark to the target row if found
- : Output parameter receiving null status of the evaluated expression
- : Output parameter indicating if the target position is outside the partition bounds

## Dependencies
- Functions called/Symbols referenced:
  - WindowObjectIsValid
  - [spool_tuples](../s/spool_tuples.md)
  - [window_gettupleslot](../w/window_gettupleslot.md)
  - [WinSetMarkPosition](WinSetMarkPosition.md)
  - ExecEvalExpr
  - [list_nth](../l/list_nth.md)
- Called from (representative examples):
  - [leadlag_common](../l/leadlag_common.md)

## Notes and Other Information
- Non-existent row positions are not treated as errors - they simply return null results
- WINDOW_SEEK_TAIL requires spooling all tuples to determine the partition size
- Uses temporary tuple slot (temp_slot_1) for efficient tuple access
- The mark position can be optionally updated as a side effect when set_mark is true
- Critical for implementing offset-based window functions like LAG, LEAD, FIRST_VALUE, and LAST_VALUE
- Provides both the evaluated result and metadata about position validity through output parameters