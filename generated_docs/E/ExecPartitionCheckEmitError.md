# ExecPartitionCheckEmitError

## Location
[src/backend/executor/execMain.c:1847-1917](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execMain.c#L1847-L1917)

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

## Simplified Source

```c
// Simplified version of ExecPartitionCheckEmitError
void ExecPartitionCheckEmitError(ResultRelInfo *resultRelInfo,
                                TupleTableSlot *slot,
                                EState *estate) {
    Oid root_relid;
    TupleDesc tupdesc;
    char *val_desc;
    Bitmapset *modifiedCols;

    // Handle tuple routing: convert partition tuple back to root table format
    if (resultRelInfo->ri_RootResultRelInfo) {
        ResultRelInfo *rootrel = resultRelInfo->ri_RootResultRelInfo;

        root_relid = RelationGetRelid(rootrel->ri_RelationDesc);
        tupdesc = RelationGetDescr(rootrel->ri_RelationDesc);

        // Build reverse attribute mapping if needed
        TupleDesc old_tupdesc = RelationGetDescr(resultRelInfo->ri_RelationDesc);
        AttrMap *map = build_attrmap_by_name_if_req(old_tupdesc, tupdesc, false);

        // Convert slot to root table format if mapping exists
        if (map != NULL) {
            slot = execute_attr_map_slot(map, slot,
                                       MakeTupleTableSlot(tupdesc, &TTSOpsVirtual));
        }

        // Get modified columns from root relation
        modifiedCols = bms_union(ExecGetInsertedCols(rootrel, estate),
                               ExecGetUpdatedCols(rootrel, estate));
    } else {
        // No tuple routing - use current relation directly
        root_relid = RelationGetRelid(resultRelInfo->ri_RelationDesc);
        tupdesc = RelationGetDescr(resultRelInfo->ri_RelationDesc);
        modifiedCols = bms_union(ExecGetInsertedCols(resultRelInfo, estate),
                               ExecGetUpdatedCols(resultRelInfo, estate));
    }

    // Build human-readable description of failing tuple values
    val_desc = ExecBuildSlotValueDescription(root_relid, slot, tupdesc,
                                           modifiedCols, 64);

    // Emit the partition constraint violation error
    ereport(ERROR,
            (errcode(ERRCODE_CHECK_VIOLATION),
             errmsg("new row for relation \"%s\" violates partition constraint",
                    RelationGetRelationName(resultRelInfo->ri_RelationDesc)),
             val_desc ? errdetail("Failing row contains %s.", val_desc) : 0,
             errtable(resultRelInfo->ri_RelationDesc)));
}
```

Key simplifications made:
- Consolidated variable declarations for clarity
- Added inline comments explaining the tuple routing logic
- Simplified the attribute mapping section with clearer variable names
- Focused on the two main execution paths (routed vs non-routed tuples)
- Preserved the essential error reporting structure
- Maintained all critical functionality while improving readability