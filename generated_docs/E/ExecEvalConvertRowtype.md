# ExecEvalConvertRowtype

## Location
src/backend/executor/execExprInterp.c: 3372 - 3466

## Overview
ExecEvalConvertRowtype performs rowtype coercion operations, converting a composite value from one record type to another, potentially rearranging field positions during the conversion.

## Definition
```c
void ExecEvalConvertRowtype(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
```

## Detailed Description
This function handles the conversion of composite types (records/rows) from one format to another. It supports both simple type ID changes (when field layouts are compatible) and complex conversions requiring field rearrangement. The function operates on the source record stored in the step's result variable and produces a converted record as output.

The conversion process involves:
1. Retrieving and caching tuple descriptors for both input and output types
2. Creating an attribute mapping when field positions differ
3. Performing either full conversion with rearrangement or simple header updates
4. Properly managing reference counts for tuple descriptors

The function handles NULL inputs by passing them through unchanged and optimizes for cases where tuples are physically compatible but need different type headers.

## Parameters / Member Variables
- `state`: Expression state context (unused in this function)
- `op`: Expression evaluation step containing conversion operation data and source/result storage
- `econtext`: Expression context providing memory context for long-lived allocations

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetHeapTupleHeader: Extracts HeapTupleHeader from input Datum
  - [get_cached_rowtype](../g/get_cached_rowtype.md): Retrieves cached tuple descriptors for input/output types
  - [IncrTupleDescRefCount](../I/IncrTupleDescRefCount.md)/DecrTupleDescRefCount: Manages tuple descriptor reference counting
  - HeapTupleHeaderGetTypeId: Gets type ID from tuple header for validation
  - [convert_tuples_by_name](../c/convert_tuples_by_name.md): Creates attribute mapping between different tuple structures
  - [execute_attr_map_tuple](../e/execute_attr_map_tuple.md): Performs tuple conversion with field rearrangement
  - [heap_copy_tuple_as_datum](../h/heap_copy_tuple_as_datum.md): Creates copy with updated composite datum headers
  - HeapTupleHeaderGetDatumLength: Gets length for HeapTupleData setup
- Called from (representative examples):
  - [ExecInterpExpr](ExecInterpExpr.md): Main expression interpreter dispatch function
  - [FunctionReturningBool](../F/FunctionReturningBool.md): JIT compilation type mapping function

## Notes and Other Information
- Supports both cases where field layout is compatible (simple header update) and where rearrangement is needed
- Uses tuple descriptor caching for performance, with change detection to rebuild conversion maps when needed
- Allocates conversion maps in per-query memory context for persistence across evaluations
- Includes assertion checking that input tuple type matches expected input descriptor or is generic RECORD type
- Handles the case where ExecEvalWholeRowVar may have changed tuple markings to plain RECORD due to alias insertion
- Assumes input composite datum doesn't contain toasted fields since it was already a composite value