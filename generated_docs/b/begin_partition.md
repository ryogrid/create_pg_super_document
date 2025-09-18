# begin_partition

## Location
[src/backend/executor/nodeWindowAgg.c:1081-1240](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeWindowAgg.c#L1081-L1240)

## Overview
This static function initializes and sets up the buffering infrastructure for processing rows of the next window partition, including creating tuplestores and read pointers for various window functions.

## Definition
```c
static void begin_partition(WindowAggState *winstate)
```

## Detailed Description
The `begin_partition` function prepares the WindowAgg execution state to begin processing a new partition of input rows. It initializes all position tracking variables, clears tuple slots, creates a new tuplestore for buffering partition data, and sets up read pointers required by different types of window functions and frame specifications.

The function handles the complex setup required for different frame options (RANGE, GROUPS, ROWS), exclusion clauses (EXCLUDE GROUP, EXCLUDE TIES), and aggregate functions. It creates specialized read pointers with appropriate capabilities (BACKWARD seeking) based on the frame specification and window function requirements.

For the very first partition, it fetches the initial input row from the outer plan. The function also stores the first tuple of the partition into the newly created tuplestore and updates the spooled row counter.

## Parameters / Member Variables
- `winstate`: The WindowAggState containing all state information for window aggregation processing, including frame options, function definitions, and position tracking

## Dependencies
- Functions called/Symbols referenced:
  - outerPlanState
  - ExecClearTuple
  - ExecProcNode
  - ExecCopySlot
  - TupIsNull
  - tuplestore_begin_heap
  - tuplestore_set_eflags
  - tuplestore_alloc_read_pointer
  - tuplestore_puttupleslot
- Called from (representative examples):
  - [ExecWindowAgg](../E/ExecWindowAgg.md)

## Notes and Other Information
- Resets all position tracking variables (currentpos, frameheadpos, frametailpos, etc.) to their initial states
- Creates read pointers conditionally based on frame options - RANGE/GROUPS modes may need special pointers for frame boundary access
- Handles aggregate functions by setting up mark/read pointers with BACKWARD capability when frame head is movable
- Sets up specialized read pointers for peer group tracking when exclusion clauses (EXCLUDE GROUP/TIES) are present
- The function assumes work_mem is available globally for tuplestore creation
- Manages memory efficiently by only creating the read pointers that are actually needed based on the window specification