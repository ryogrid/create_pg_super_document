# ordered_set_shutdown

## Location
[src/backend/utils/adt/orderedsetaggs.c:339-357](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/orderedsetaggs.c#L339-L357)

## Overview
Cleans up resources when evaluation of an ordered-set aggregate is complete, ensuring proper cleanup of tuplesort objects and tuple slots.

## Definition


## Detailed Description
The  function serves as a cleanup callback for ordered-set aggregates. It is registered during  and is automatically called by the aggregate framework when a group's processing is complete. The function ensures that any non-memory resources (particularly temporary files used by tuplesort) are properly released.

The function does not need to free memory allocated in the per-group context since that context will be reset by nodeAgg.c, nor does it free per-query context memory which is handled by ExecutorEnd. Instead, it focuses on releasing system resources like temporary files and clearing tuple slots that might be holding buffer pins.

## Parameters / Member Variables
- : Datum containing a pointer to the OSAPerGroupState structure that needs cleanup

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetPointer](../D/DatumGetPointer.md)
  - tuplesort_end
  - ExecClearTuple
- Called from (representative examples):
  - Registered as callback in ordered_set_startup (called by aggregate framework)

## Notes and Other Information
- Registered as a shutdown callback via AggRegisterCallback in ordered_set_startup
- Called automatically by the aggregate execution framework at the end of each group
- Designed to be safe even when called multiple times or when resources are already cleaned up
- Does not free memory contexts since they are managed by the aggregate execution framework
- Essential for preventing temporary file leaks in long-running queries with many groups