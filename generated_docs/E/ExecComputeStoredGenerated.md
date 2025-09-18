# ExecComputeStoredGenerated

## Location
[src/backend/executor/nodeModifyTable.c:473-568](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L473-L568)

## Overview
Computes stored generated columns for a tuple by evaluating their generation expressions and updating the tuple slot with the computed values.

## Definition


## Detailed Description
This function handles the computation of stored generated columns during INSERT and UPDATE operations. Generated columns are virtual columns whose values are computed based on expressions that reference other columns in the same table. The function evaluates these expressions and materializes the computed values in the tuple slot.

The function operates in two main phases:
1. **Initialization**: Checks if the generated expressions have been initialized and determines which expressions need to be computed based on the command type (INSERT vs UPDATE)
2. **Computation**: Evaluates each generated column expression, copies the results to appropriate memory contexts, and updates the tuple slot

Key behaviors:
- For UPDATE operations, only computes generated columns that may have changed
- For INSERT operations, computes all stored generated columns
- Handles memory management by copying computed values to the per-tuple memory context
- Materializes the updated slot to ensure data consistency

## Parameters / Member Variables
- : Result relation information containing cached generated expressions and metadata
- : Executor state providing memory contexts and expression evaluation infrastructure  
- : Tuple table slot containing the tuple data to be updated with generated values
- : Command type (CMD_INSERT, CMD_UPDATE) determining which generated expressions to evaluate

## Dependencies
- Functions called/Symbols referenced:
  - GetPerTupleExprContext
  - [ExecInitStoredGenerated](ExecInitStoredGenerated.md)  
  - GetPerTupleMemoryContext
  - slot_getallattrs
  - ExecEvalExpr
  - [datumCopy](../d/datumCopy.md)
  - ExecClearTuple
  - [ExecStoreVirtualTuple](ExecStoreVirtualTuple.md)
  - ExecMaterializeSlot
- Called from (representative examples):
  - [CopyFrom](../C/CopyFrom.md)
  - [ExecSimpleRelationInsert](ExecSimpleRelationInsert.md)
  - [ExecSimpleRelationUpdate](ExecSimpleRelationUpdate.md)
  - [ExecInsert](ExecInsert.md)
  - [ExecUpdatePrepareSlot](ExecUpdatePrepareSlot.md)

## Notes and Other Information
- The function assumes the relation has stored generated columns (asserted via tupdesc->constr->has_generated_stored)
- Different expression arrays are maintained for INSERT (ri_GeneratedExprsI) vs UPDATE (ri_GeneratedExprsU) operations
- Memory management is critical - computed values are copied using datumCopy to ensure they persist beyond expression evaluation
- The function materializes the slot after updating to ensure the computed values are properly stored
- Early exit optimization for UPDATE operations when no generated columns need recomputation