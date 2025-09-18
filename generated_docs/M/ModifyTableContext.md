# ModifyTableContext

## Location
src/backend/executor/nodeModifyTable.c: 86 - 110

## Overview
ModifyTableContext is a context structure that encapsulates the basic execution state and output variables for ModifyTable operations, providing a centralized way to pass execution context to various helper functions during INSERT, UPDATE, DELETE, and MERGE operations.

## Definition


## Detailed Description
This structure serves as a context container that bundles together all the essential state and execution information needed during ModifyTable operations. It is primarily used to simplify function signatures by avoiding the need to pass numerous individual parameters to helper functions like ExecUpdateAct(), ExecDeleteAct(), and related routines.

The context structure encapsulates the main execution states, tuple slots for data access, and specialized fields for handling concurrent modifications and cross-partition operations. It acts as a communication medium between the main ModifyTable execution logic and its various helper functions, allowing them to share state information and return results effectively.

## Parameters / Member Variables
- : Pointer to the ModifyTableState containing the primary execution state for the ModifyTable operation
- : Pointer to EPQState used for EvalPlanQual rechecks when handling concurrent tuple modifications
- : Pointer to the executor state containing query execution context and metadata
- : TupleTableSlot containing the tuple from ModifyTable's subplan, used to access junk columns that won't be stored
- : TM_FailureData structure containing information about concurrent changes made to a tuple being updated or deleted
- : TupleTableSlot for holding the tuple projected by INSERT's RETURNING clause during cross-partition UPDATE operations

## Dependencies
- Functions called/Symbols referenced:
  - ModifyTableState
  - EPQState  
  - TM_FailureData
- Called from (representative examples):
  - ExecInsert
  - ExecUpdate
  - ExecDelete
  - ExecMerge
  - ExecUpdateAct
  - ExecDeleteAct
  - ExecCrossPartitionUpdate
  - ExecOnConflictUpdate

## Notes and Other Information
- This context structure was introduced to consolidate the numerous parameters that were previously passed individually to ModifyTable helper functions
- The tmfd field is crucial for handling concurrent modifications and provides detailed information about what transaction modified a tuple and when
- The cpUpdateReturningSlot is specifically used in cross-partition UPDATE scenarios where a tuple needs to be moved between partitions
- The structure helps maintain clean interfaces between the main ModifyTable execution logic and its specialized helper functions
- It's widely used throughout the ModifyTable execution path, from the main ExecModifyTable function down to specific action handlers