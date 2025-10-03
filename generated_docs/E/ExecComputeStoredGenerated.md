# ExecComputeStoredGenerated

## Location
[src/backend/executor/nodeModifyTable.c:473-568](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L473-L568)

## Overview
Computes stored generated columns for a tuple by evaluating their generation expressions and updating the tuple slot with the computed values.

## Definition

```c
void
ExecComputeStoredGenerated(ResultRelInfo *resultRelInfo,
						   EState *estate, TupleTableSlot *slot,
						   CmdType cmdtype)
```
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
- `*resultRelInfo`: Result relation information containing cached generated expressions and metadata
- `*estate`: Executor state providing memory contexts and expression evaluation infrastructure
- `*slot`: Tuple table slot containing the tuple data to be updated with generated values
- `cmdtype`: Command type (CMD_INSERT, CMD_UPDATE) determining which generated expressions to evaluate
## Dependencies
- Functions called/Symbols referenced:
  - GetPerTupleExprContext
  - [ExecInitStoredGenerated](ExecInitStoredGenerated.md)  
  - GetPerTupleMemoryContext
  - [slot_getallattrs](../s/slot_getallattrs.md)
  - [ExecEvalExpr](ExecEvalExpr.md)
  - [datumCopy](../d/datumCopy.md)
  - [ExecClearTuple](ExecClearTuple.md)
  - [ExecStoreVirtualTuple](ExecStoreVirtualTuple.md)
  - [ExecMaterializeSlot](ExecMaterializeSlot.md)
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

## Simplified Source

```c
void
ExecComputeStoredGenerated(ResultRelInfo *resultRelInfo, EState *estate,
                          TupleTableSlot *slot, CmdType cmdtype)
{
    Relation rel = resultRelInfo->ri_RelationDesc;
    TupleDesc tupdesc = RelationGetDescr(rel);
    int natts = tupdesc->natts;
    ExprContext *econtext = GetPerTupleExprContext(estate);
    ExprState **ri_GeneratedExprs;
    MemoryContext oldContext;
    Datum *values;
    bool *nulls;

    // Must have stored generated columns
    Assert(tupdesc->constr && tupdesc->constr->has_generated_stored);

    // Initialize expressions if needed and check for early exit
    if (cmdtype == CMD_UPDATE)
    {
        if (resultRelInfo->ri_GeneratedExprsU == NULL)
            ExecInitStoredGenerated(resultRelInfo, estate, cmdtype);
        if (resultRelInfo->ri_NumGeneratedNeededU == 0)
            return;
        ri_GeneratedExprs = resultRelInfo->ri_GeneratedExprsU;
    }
    else
    {
        if (resultRelInfo->ri_GeneratedExprsI == NULL)
            ExecInitStoredGenerated(resultRelInfo, estate, cmdtype);
        ri_GeneratedExprs = resultRelInfo->ri_GeneratedExprsI;
    }

    // Switch to per-tuple memory context
    oldContext = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

    // Allocate arrays for computed values
    values = palloc(sizeof(*values) * natts);
    nulls = palloc(sizeof(*nulls) * natts);

    // Get current tuple data
    slot_getallattrs(slot);
    memcpy(nulls, slot->tts_isnull, sizeof(*nulls) * natts);

    // Compute generated columns
    for (int i = 0; i < natts; i++)
    {
        Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

        if (ri_GeneratedExprs[i])
        {
            // Compute generated column value
            econtext->ecxt_scantuple = slot;
            Datum val = ExecEvalExpr(ri_GeneratedExprs[i], econtext, &nulls[i]);

            // Copy value to ensure it persists
            if (!nulls[i])
                val = datumCopy(val, attr->attbyval, attr->attlen);

            values[i] = val;
        }
        else
        {
            // Copy existing non-generated value
            if (!nulls[i])
                values[i] = datumCopy(slot->tts_values[i], attr->attbyval, attr->attlen);
        }
    }

    // Update slot with computed values
    ExecClearTuple(slot);
    memcpy(slot->tts_values, values, sizeof(*values) * natts);
    memcpy(slot->tts_isnull, nulls, sizeof(*nulls) * natts);
    ExecStoreVirtualTuple(slot);
    ExecMaterializeSlot(slot);

    MemoryContextSwitchTo(oldContext);
}
```