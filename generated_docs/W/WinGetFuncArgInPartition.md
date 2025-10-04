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
- `winobj`: Window object containing partition data and state
- `argno`: Zero-based index of the argument expression to evaluate
- `relpos`: Signed offset from the seek position
- `seektype`: Position reference point (WINDOW_SEEK_CURRENT, WINDOW_SEEK_HEAD, or WINDOW_SEEK_TAIL)
- `set_mark`: Whether to move the mark to the target row if found
- `*isnull`: Output parameter receiving null status of the evaluated expression
- `*isout`: Output parameter indicating if the target position is outside the partition bounds
## Dependencies
- Functions called/Symbols referenced:
  - WindowObjectIsValid
  - [spool_tuples](../s/spool_tuples.md)
  - [window_gettupleslot](../w/window_gettupleslot.md)
  - [WinSetMarkPosition](WinSetMarkPosition.md)
  - [ExecEvalExpr](../E/ExecEvalExpr.md)
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

## Simplified Source

```c
Datum
WinGetFuncArgInPartition(WindowObject winobj, int argno, int relpos, int seektype,
                        bool set_mark, bool *isnull, bool *isout)
{
    WindowAggState *winstate;
    ExprContext *econtext;
    TupleTableSlot *slot;
    bool gottuple;
    int64 abs_pos;

    // Validate window object and get state
    Assert(WindowObjectIsValid(winobj));
    winstate = winobj->winstate;
    econtext = winstate->ss.ps.ps_ExprContext;
    slot = winstate->temp_slot_1;

    // Calculate absolute position based on seek type
    switch (seektype)
    {
        case WINDOW_SEEK_CURRENT:
            abs_pos = winstate->currentpos + relpos;
            break;
        case WINDOW_SEEK_HEAD:
            abs_pos = relpos;
            break;
        case WINDOW_SEEK_TAIL:
            spool_tuples(winstate, -1);  // Spool all tuples
            abs_pos = winstate->spooled_rows - 1 + relpos;
            break;
        default:
            elog(ERROR, "unrecognized window seek type: %d", seektype);
            abs_pos = 0;  // Keep compiler quiet
            break;
    }

    // Try to get the tuple at the calculated position
    gottuple = window_gettupleslot(winobj, abs_pos, slot);

    if (!gottuple)
    {
        // Position is out of bounds
        if (isout)
            *isout = true;
        *isnull = true;
        return (Datum) 0;
    }
    else
    {
        // Evaluate argument expression on the fetched tuple
        if (isout)
            *isout = false;
        if (set_mark)
            WinSetMarkPosition(winobj, abs_pos);

        econtext->ecxt_outertuple = slot;
        return ExecEvalExpr((ExprState *) list_nth(winobj->argstates, argno),
                           econtext, isnull);
    }
}
```