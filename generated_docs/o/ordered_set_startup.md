# ordered_set_startup

## Location
[src/backend/utils/adt/orderedsetaggs.c:113-338](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/orderedsetaggs.c#L113-L338)

## Overview
Sets up working state for an ordered-set aggregate function, initializing per-query and per-group state structures required for sorting and managing aggregated data.

## Definition


## Detailed Description
The  function initializes the necessary state structures for ordered-set aggregate functions like , , and . It performs both per-query initialization (done once per query) and per-group initialization (done once per aggregate group).

The function first validates that it's being called in an aggregate context, then sets up a per-query state structure if it doesn't already exist. This includes analyzing the aggregate's sort requirements and preparing tuple descriptors or datum sorting information. Finally, it creates a per-group state structure with an initialized tuplesort object for collecting and sorting the aggregated values.

For hypothetical-set aggregates, it adds a special flag column to distinguish between regular input rows and the hypothetical row. The function supports both tuple-based sorting (for complex aggregates with multiple columns) and datum-based sorting (for simple single-column aggregates).

## Parameters / Member Variables
- : Function call information containing aggregate context and metadata
- : Boolean flag indicating whether to use tuple-based sorting (true) or datum-based sorting (false)

## Dependencies
- Functions called/Symbols referenced:
  - [AggCheckCallContext](../A/AggCheckCallContext.md)
  - [AggGetAggref](../A/AggGetAggref.md)
  - [AggStateIsShared](../A/AggStateIsShared.md)
  - [AggRegisterCallback](../A/AggRegisterCallback.md)
  - [tuplesort_begin_heap](../t/tuplesort_begin_heap.md)
  - [tuplesort_begin_datum](../t/tuplesort_begin_datum.md)
  - [ExecTypeFromTL](../E/ExecTypeFromTL.md)
  - [get_sortgroupclause_tle](../g/get_sortgroupclause_tle.md)
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md)
  - [ordered_set_shutdown](ordered_set_shutdown.md)
- Called from (representative examples):
  - [ordered_set_transition](ordered_set_transition.md)
  - [ordered_set_transition_multi](ordered_set_transition_multi.md)

## Notes and Other Information
- The function maintains two levels of state: per-query state (cached in ) and per-group state (allocated in group-lifespan memory context)
- Supports rescanning if the aggregate state is shared across multiple execution nodes
- Registers a shutdown callback to clean up resources at the end of each group
- Handles both regular ordered-set aggregates and hypothetical-set aggregates with special flag column logic
- Uses  to configure the tuplesort memory usage limit