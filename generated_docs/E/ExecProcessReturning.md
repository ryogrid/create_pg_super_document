# ExecProcessReturning

## Location
src/backend/executor/nodeModifyTable.c: 277 - 308

## Overview
Evaluates a RETURNING clause expression list and returns the computed result tuple for INSERT, UPDATE, DELETE, and MERGE operations.

## Definition
```c
static TupleTableSlot *ExecProcessReturning(ResultRelInfo *resultRelInfo, TupleTableSlot *tupleSlot, TupleTableSlot *planSlot)
```

## Detailed Description
ExecProcessReturning handles the evaluation of RETURNING expressions in DML operations. It sets up the expression context with the appropriate tuple data and computes the RETURNING clause results using the projection mechanism.

The function handles different scenarios:
- For regular operations, it uses the provided tupleSlot containing the actual inserted/updated/deleted tuple
- For FDW operations, it can work with tupleSlot being NULL, in which case the FDW should have already set up the scan tuple in the expression context
- It ensures the tableoid system column is properly initialized for RETURNING expressions that might reference it

The function leverages PostgreSQL's projection infrastructure to efficiently compute the RETURNING expressions and return the result tuple.

## Parameters / Member Variables
- `resultRelInfo`: Information about the current result relation containing projection details
- `tupleSlot`: Slot holding the tuple that was actually inserted/updated/deleted (can be NULL for FDW cases)
- `planSlot`: Slot holding the tuple returned by the top subplan node

## Dependencies
- Functions called/Symbols referenced:
  - [ProjectionInfo](../P/ProjectionInfo.md) (struct type)
  - ExecProject
  - RelationGetRelid
- Called from (representative examples):
  - [ExecInsert](ExecInsert.md)
  - [ExecDelete](ExecDelete.md)
  - [ExecUpdate](ExecUpdate.md)
  - [ExecMergeMatched](ExecMergeMatched.md)
  - [ExecModifyTable](ExecModifyTable.md)

## Notes and Other Information
- This function is static to nodeModifyTable.c and used internally during DML execution
- The function properly handles the tableoid system column by reinitializing it before expression evaluation
- For FDW operations, the function can work with NULL tupleSlot, relying on the FDW to have set up the expression context
- The function returns a TupleTableSlot containing the computed RETURNING expression results
- This is a key component in PostgreSQL's RETURNING clause implementation across all DML operations