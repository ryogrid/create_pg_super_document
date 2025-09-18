# ExecEvalWholeRowVar

## Location
src/backend/executor/execExprInterp.c: 4770 - 4996

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
  - ExecFilterJunk
  - [lookup_rowtype_tupdesc_domain](../l/lookup_rowtype_tupdesc_domain.md)
  - [CreateTupleDescCopy](../C/CreateTupleDescCopy.md)
  - ReleaseTupleDesc
  - exec_rt_fetch
  - [ExecTypeSetColNames](ExecTypeSetColNames.md)
  - [BlessTupleDesc](../B/BlessTupleDesc.md)
  - slot_getallattrs
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