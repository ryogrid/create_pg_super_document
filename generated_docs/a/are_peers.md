# are_peers

## Location
[src/backend/executor/nodeWindowAgg.c:3043-3065](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeWindowAgg.c#L3043-L3065)

## Overview
are_peers compares two rows to determine if they are equal according to the ORDER BY clause in a window function definition.

## Definition
```c
static bool are_peers(WindowAggState *winstate, TupleTableSlot *slot1, TupleTableSlot *slot2)
```

## Detailed Description
are_peers is a fundamental comparison function used throughout window function processing to determine if two rows should be considered equivalent for partitioning and ordering purposes. The function handles two key scenarios: 1) If no ORDER BY clause exists in the window specification (ordNumCols == 0), all rows within a partition are considered peers and the function returns true, and 2) When an ORDER BY clause is present, the function sets up the expression context with the two tuples and uses the pre-compiled equality function (ordEqfunction) to perform the comparison. This function is critical for determining window frame boundaries, peer groups, and when aggregate states need to be updated or reset. Note that this comparison only considers the ORDER BY clause and does not factor in window frame mode specifications.

## Parameters / Member Variables
- `winstate`: WindowAggState containing the execution state and equality function
- `slot1`: First tuple slot to compare
- `slot2`: Second tuple slot to compare

## Dependencies
- Functions called/Symbols referenced:
  - ExecQualAndReset
  - WindowAgg (plan node structure)
- Called from (representative examples):
  - row_is_in_frame (frame boundary determination)
  - update_frameheadpos, update_frametailpos (frame position updates)
  - update_grouptailpos (group boundary tracking)
  - ExecWindowAgg (main execution logic)
  - WinRowsArePeers (external interface)

## Notes and Other Information
- This function does not consider window frame mode (ROWS vs RANGE vs GROUPS)
- Uses the temporary expression context (tmpcontext) to avoid memory leaks
- The ordEqfunction is pre-compiled during initialization for efficiency
- Essential for RANGE and GROUPS frame mode where peer detection is required
- Located in src/backend/executor/nodeWindowAgg.c:3043-3065