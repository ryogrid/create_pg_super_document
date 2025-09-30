# ExecEvalWholeRowVar

## Location
[src/backend/executor/execExprInterp.c:4770-4996](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L4770-L4996)

## Overview
ExecEvalWholeRowVar evaluates whole-row variable expressions, constructing composite datum values that represent entire table rows or tuple slots in PostgreSQL's expression evaluation system.

## Definition
void ExecEvalWholeRowVar(ExprState *state, ExprEvalStep *op, ExprContext *econtext)

## Detailed Description
This function handles the evaluation of whole-row Var expressions, which represent references to entire table rows rather than individual columns. When a query references a table name without specifying columns (like "SELECT tablename FROM tablename"), PostgreSQL creates whole-row variables that need to be materialized as composite values.

The function performs several complex operations:

1. **Slot Selection**: Determines which tuple slot to use based on the variable's varno (INNER_VAR, OUTER_VAR, or scan tuple)

2. **Junk Filtering**: Applies any necessary junk filters to clean the tuple data

3. **Type Compatibility Checking**: On first execution, validates that the actual tuple structure matches the expected composite type, handling dropped columns and type mismatches appropriately

4. **Tuple Descriptor Management**: Creates and manages tuple descriptors for the output, handling both named composite types and RECORD types differently

5. **Column Name Resolution**: For RECORD types, attempts to resolve proper column names from the range table entry

6. **Slow Path Handling**: For cases with dropped columns that have storage mismatches, performs additional runtime validation

7. **Composite Construction**: Builds the final composite datum using toast_build_flattened_tuple to handle TOAST values properly

The function maintains performance optimizations while ensuring type safety and proper handling of PostgreSQL's complex type system including dropped columns, domains, and TOAST values.

## Parameters / Member Variables
- : The ExprState containing the expression evaluation context
- : The ExprEvalStep operation descriptor containing wholerow-specific data including var, junkFilter, tupdesc, and flags
- : The ExprContext providing access to tuple slots (inner, outer, scan) and execution state

## Dependencies
- Functions called/Symbols referenced:
  - [ExecFilterJunk](ExecFilterJunk.md)
  - [lookup_rowtype_tupdesc_domain](../l/lookup_rowtype_tupdesc_domain.md)
  - [CreateTupleDescCopy](../C/CreateTupleDescCopy.md)
  - ReleaseTupleDesc
  - [exec_rt_fetch](../e/exec_rt_fetch.md)
  - [ExecTypeSetColNames](ExecTypeSetColNames.md)
  - [BlessTupleDesc](../B/BlessTupleDesc.md)
  - [slot_getallattrs](../s/slot_getallattrs.md)
  - [toast_build_flattened_tuple](../t/toast_build_flattened_tuple.md)
  - HeapTupleHeaderSetTypeId
  - HeapTupleHeaderSetTypMod
  - [PointerGetDatum](../P/PointerGetDatum.md)
- Called from (representative examples):
  - [ExecInterpExpr](ExecInterpExpr.md) (main expression interpreter loop)

## Notes and Other Information
- The function uses a "first time through" optimization to cache type compatibility information
- Handles complex scenarios involving dropped columns, domain types, and storage layout mismatches
- The slow path is triggered when dropped columns have different storage characteristics
- Supports both named composite types and generic RECORD types with different handling strategies
- Critical for implementing PostgreSQL's whole-row variable semantics in SQL queries
- The function must handle TOAST values correctly by flattening them in the composite datum
- Column name resolution for RECORD types attempts to use aliases from range table entries when available

## Simplified Source

```c
void ExecEvalWholeRowVar(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
{
    Var *variable = op->d.wholerow.var;
    TupleTableSlot *slot;

    // Get appropriate tuple slot based on variable type
    switch (variable->varno) {
        case INNER_VAR:
            slot = econtext->ecxt_innertuple;
            break;
        case OUTER_VAR:
            slot = econtext->ecxt_outertuple;
            break;
        default:
            slot = econtext->ecxt_scantuple;
            break;
    }

    // Apply junk filter if needed
    if (op->d.wholerow.junkFilter != NULL)
        slot = ExecFilterJunk(op->d.wholerow.junkFilter, slot);

    // First-time setup: validate type compatibility and create tuple descriptor
    if (op->d.wholerow.first) {
        TupleDesc output_tupdesc;

        if (variable->vartype != RECORDOID) {
            // Named composite type: validate compatibility
            TupleDesc var_tupdesc = lookup_rowtype_tupdesc_domain(variable->vartype, -1, false);
            TupleDesc slot_tupdesc = slot->tts_tupleDescriptor;

            // Check attribute count and types match
            if (var_tupdesc->natts != slot_tupdesc->natts)
                ereport(ERROR, "table row type and query-specified row type do not match");

            // Validate each attribute type
            for (int i = 0; i < var_tupdesc->natts; i++) {
                Form_pg_attribute vattr = TupleDescAttr(var_tupdesc, i);
                Form_pg_attribute sattr = TupleDescAttr(slot_tupdesc, i);

                if (vattr->atttypid != sattr->atttypid && !vattr->attisdropped)
                    ereport(ERROR, "table row type and query-specified row type do not match");

                // Mark as slow if dropped columns have storage mismatches
                if (vattr->attisdropped &&
                    (vattr->attlen != sattr->attlen || vattr->attalign != sattr->attalign))
                    op->d.wholerow.slow = true;
            }

            output_tupdesc = CreateTupleDescCopy(var_tupdesc);
            ReleaseTupleDesc(var_tupdesc);
        } else {
            // RECORD type: use slot's descriptor and resolve column names
            output_tupdesc = CreateTupleDescCopy(slot->tts_tupleDescriptor);
            output_tupdesc->tdtypeid = RECORDOID;
            output_tupdesc->tdtypmod = -1;

            // Try to get column names from range table entry
            if (econtext->ecxt_estate && variable->varno <= econtext->ecxt_estate->es_range_table_size) {
                RangeTblEntry *rte = exec_rt_fetch(variable->varno, econtext->ecxt_estate);
                if (rte->eref)
                    ExecTypeSetColNames(output_tupdesc, rte->eref->colnames);
            }
        }

        op->d.wholerow.tupdesc = BlessTupleDesc(output_tupdesc);
        op->d.wholerow.first = false;
    }

    // Ensure all slot attributes are accessible
    slot_getallattrs(slot);

    // Slow path: validate dropped columns if needed
    if (op->d.wholerow.slow) {
        TupleDesc var_tupdesc = op->d.wholerow.tupdesc;
        for (int i = 0; i < var_tupdesc->natts; i++) {
            Form_pg_attribute vattr = TupleDescAttr(var_tupdesc, i);
            if (vattr->attisdropped && !slot->tts_isnull[i]) {
                // Validate storage compatibility for non-null dropped columns
                Form_pg_attribute sattr = TupleDescAttr(slot->tts_tupleDescriptor, i);
                if (vattr->attlen != sattr->attlen || vattr->attalign != sattr->attalign)
                    ereport(ERROR, "Physical storage mismatch on dropped attribute");
            }
        }
    }

    // Build composite datum with flattened TOAST values
    HeapTuple tuple = toast_build_flattened_tuple(slot->tts_tupleDescriptor,
                                                  slot->tts_values,
                                                  slot->tts_isnull);

    // Set type information and return result
    HeapTupleHeader dtuple = tuple->t_data;
    HeapTupleHeaderSetTypeId(dtuple, op->d.wholerow.tupdesc->tdtypeid);
    HeapTupleHeaderSetTypMod(dtuple, op->d.wholerow.tupdesc->tdtypmod);

    *op->resvalue = PointerGetDatum(dtuple);
    *op->resnull = false;
}
```