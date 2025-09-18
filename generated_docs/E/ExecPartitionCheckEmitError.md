# ExecPartitionCheckEmitError

## Location
src/backend/executor/execMain.c: 1847 - 1917

## Overview
Generates and emits a detailed error message when a tuple fails a partition constraint check, handling tuple format conversion for routed tuples.

## Definition
```c
void ExecPartitionCheckEmitError(ResultRelInfo *resultRelInfo,
                                TupleTableSlot *slot,
                                EState *estate)
```

## Detailed Description
ExecPartitionCheckEmitError is responsible for constructing informative error messages when partition constraint violations occur. The function handles the complexity of tuple routing by converting partition-specific tuple formats back to the root table's rowtype to ensure error messages accurately reflect the original input data. It builds a comprehensive column bitmap representing both inserted and updated columns, then generates a human-readable description of the failing tuple values. The error is reported with appropriate error codes and includes both the constraint violation message and detailed information about the failing row contents.

## Parameters / Member Variables
- `resultRelInfo`: ResultRelInfo structure for the target partition relation, may contain reference to root relation for routed tuples
- `slot`: TupleTableSlot containing the tuple that failed the partition constraint check
- `estate`: Execution state providing access to column modification tracking and execution context

## Dependencies
- Functions called/Symbols referenced:
  - [build_attrmap_by_name_if_req](../b/build_attrmap_by_name_if_req.md)
  - [execute_attr_map_slot](../e/execute_attr_map_slot.md)
  - [MakeTupleTableSlot](../M/MakeTupleTableSlot.md)
  - [ExecGetInsertedCols](ExecGetInsertedCols.md)
  - [ExecGetUpdatedCols](ExecGetUpdatedCols.md)
  - [bms_union](../b/bms_union.md)
  - [ExecBuildSlotValueDescription](ExecBuildSlotValueDescription.md)
  - [errtable](../e/errtable.md)
- Called from (representative examples):
  - [ExecPartitionCheck](ExecPartitionCheck.md)
  - [ExecCrossPartitionUpdate](ExecCrossPartitionUpdate.md)

## Notes and Other Information
- Handles tuple format conversion when dealing with routed tuples that have been converted to partition-specific rowtypes
- Uses reverse attribute mapping to convert partition tuples back to root table format for consistent error reporting
- Combines inserted and updated column bitmaps to provide comprehensive tuple value descriptions in error messages
- Generates structured error reports with ERRCODE_CHECK_VIOLATION and includes table context
- Limits tuple value description to 64 characters for readability while providing essential debugging information