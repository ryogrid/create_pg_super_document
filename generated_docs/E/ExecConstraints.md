# ExecConstraints

## Location
[src/backend/executor/execMain.c:1918-2052](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execMain.c#L1918-L2052)

## Overview
Validates traditional NOT NULL and check constraints for a tuple, handling tuple format conversion for partitioned tables but excluding partition constraints.

## Definition
```c
void ExecConstraints(ResultRelInfo *resultRelInfo,
                    TupleTableSlot *slot, EState *estate)
```

## Detailed Description
ExecConstraints is the primary function for enforcing traditional table constraints (NOT NULL and check constraints) during tuple insertion and updates. The function operates in two phases: first validating NOT NULL constraints by iterating through all attributes with NOT NULL requirements, then evaluating check constraints using ExecRelCheck. For partitioned tables, it handles tuple format conversion by mapping partition-specific tuple formats back to root table formats to ensure error messages accurately reflect the original input data. The function explicitly excludes partition constraints, which are handled separately by ExecPartitionCheck.

## Parameters / Member Variables
- `resultRelInfo`: ResultRelInfo structure for the target relation, containing constraint metadata and potential root relation reference
- `slot`: TupleTableSlot containing the tuple to be validated against table constraints  
- `estate`: Execution state providing access to column modification tracking and execution context

## Dependencies
- Functions called/Symbols referenced:
  - slot_attisnull
  - [build_attrmap_by_name_if_req](../b/build_attrmap_by_name_if_req.md)
  - [execute_attr_map_slot](../e/execute_attr_map_slot.md)
  - [MakeTupleTableSlot](../M/MakeTupleTableSlot.md)
  - [ExecGetInsertedCols](ExecGetInsertedCols.md)
  - [ExecGetUpdatedCols](ExecGetUpdatedCols.md)
  - [bms_union](../b/bms_union.md)
  - [ExecBuildSlotValueDescription](ExecBuildSlotValueDescription.md)
  - [ExecRelCheck](ExecRelCheck.md)
  - [errtablecol](../e/errtablecol.md)
  - [errtableconstraint](../e/errtableconstraint.md)
- Called from (representative examples):
  - [CopyFrom](../C/CopyFrom.md)
  - [ExecSimpleRelationInsert](ExecSimpleRelationInsert.md)
  - [ExecSimpleRelationUpdate](ExecSimpleRelationUpdate.md)
  - [ExecInsert](ExecInsert.md)
  - [ExecUpdateAct](ExecUpdateAct.md)

## Notes and Other Information
- Explicitly excludes partition constraint validation, which is handled by ExecPartitionCheck
- Performs tuple format conversion for routed tuples to ensure error messages match original input format
- Validates NOT NULL constraints by iterating through all table attributes with NOT NULL requirements
- Check constraints are evaluated using ExecRelCheck, which returns the name of the first failed constraint
- Generates comprehensive error messages including tuple value descriptions limited to 64 characters
- Uses appropriate error codes: ERRCODE_NOT_NULL_VIOLATION for NOT NULL violations and ERRCODE_CHECK_VIOLATION for check constraint failures
- Handles both root tables and partitioned table scenarios with appropriate attribute mapping