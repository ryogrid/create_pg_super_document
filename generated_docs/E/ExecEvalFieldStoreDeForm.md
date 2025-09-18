# ExecEvalFieldStoreDeForm

## Location
[src/backend/executor/execExprInterp.c:3298-3347](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L3298-L3347)

## Overview
Deforms a source tuple into individual field values and null flags as the first step of a FieldStore expression, preparing for subsequent field updates and tuple reconstruction.

## Definition
```c
void ExecEvalFieldStoreDeForm(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
```

## Detailed Description
ExecEvalFieldStoreDeForm is the first phase of FieldStore expression evaluation in PostgreSQL's expression system. FieldStore expressions modify specific fields within composite types while preserving other fields. This function decomposes the source tuple into individual field values and null indicators, storing them in arrays that subsequent evaluation steps can selectively modify.

The function handles two scenarios: NULL input tuples (converted to all-NULL field arrays) and valid tuples (deformed using heap_deform_tuple). It includes validation to ensure the tuple descriptor doesn't exceed the allocated array space and uses cached tuple descriptors for performance.

After this deformation step, subsequent evaluation steps will overwrite specific array elements with new field values, and finally FIELDSTORE_FORM will reconstruct the modified tuple from the arrays.

## Parameters / Member Variables
- `state`: ExprState containing the overall expression evaluation context (currently unused)
- `econtext`: ExprContext providing runtime evaluation context (currently unused)
- `op`: ExprEvalStep containing the specific operation data including:
  - `op->resnull`: Pointer to NULL flag of the source tuple
  - `op->resvalue`: Pointer to the source tuple Datum
  - `op->d.fieldstore.nulls`: Array to store null flags for each field
  - `op->d.fieldstore.values`: Array to store Datum values for each field
  - `op->d.fieldstore.ncolumns`: Number of columns allocated in the arrays
  - `op->d.fieldstore.fstore`: FieldStore node containing result type information
  - `op->d.fieldstore.rowcache`: Cache for tuple descriptor lookups

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetHeapTupleHeader: Converts Datum to HeapTupleHeader
  - HeapTupleHeaderGetDatumLength: Gets tuple length for HeapTuple construction
  - [ItemPointerSetInvalid](../I/ItemPointerSetInvalid.md): Sets invalid item pointer for temporary tuple
  - [get_cached_rowtype](../g/get_cached_rowtype.md): Retrieves cached tuple descriptor for the result type
  - [heap_deform_tuple](../h/heap_deform_tuple.md): Decomposes tuple into individual field values and null flags
- Called from (representative examples):
  - [ExecInterpExpr](ExecInterpExpr.md): Main expression interpreter dispatch function
  - [FunctionReturningBool](../F/FunctionReturningBool.md): JIT compilation context

## Notes and Other Information
- This is the first step in a multi-step FieldStore evaluation process
- NULL input tuples are converted to all-NULL field arrays rather than errors
- Uses tuple descriptor caching for performance optimization
- Includes validation to prevent buffer overflows from mismatched tuple descriptors
- The deformation creates temporary arrays that subsequent steps will modify selectively
- Part of PostgreSQL's compiled expression evaluation system for efficient tuple modification
- Must be followed by field update steps and FIELDSTORE_FORM to complete the operation
- Uses HeapTupleData wrapper to interface with heap_deform_tuple function requirements