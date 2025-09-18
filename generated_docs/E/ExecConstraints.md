# ExecConstraints

## Location
src/backend/executor/execMain.c: 1918 - 2052

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
  - build_attrmap_by_name_if_req
  - execute_attr_map_slot
  - MakeTupleTableSlot
  - ExecGetInsertedCols
  - ExecGetUpdatedCols
  - bms_union
  - ExecBuildSlotValueDescription
  - ExecRelCheck
  - errtablecol
  - errtableconstraint
- Called from (representative examples):
  - CopyFrom
  - ExecSimpleRelationInsert
  - ExecSimpleRelationUpdate
  - ExecInsert
  - ExecUpdateAct

## Notes and Other Information
- Explicitly excludes partition constraint validation, which is handled by ExecPartitionCheck
- Performs tuple format conversion for routed tuples to ensure error messages match original input format
- Validates NOT NULL constraints by iterating through all table attributes with NOT NULL requirements
- Check constraints are evaluated using ExecRelCheck, which returns the name of the first failed constraint
- Generates comprehensive error messages including tuple value descriptions limited to 64 characters
- Uses appropriate error codes: ERRCODE_NOT_NULL_VIOLATION for NOT NULL violations and ERRCODE_CHECK_VIOLATION for check constraint failures
- Handles both root tables and partitioned table scenarios with appropriate attribute mapping