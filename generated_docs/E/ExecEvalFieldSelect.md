# ExecEvalFieldSelect

## Location
src/backend/executor/execExprInterp.c: 3173 - 3297

## Overview
Evaluates a FieldSelect node by extracting a specific field from a composite/record type, handling both expanded records and standard heap tuples with type validation and error checking.

## Definition
```c
void ExecEvalFieldSelect(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
```

## Detailed Description
ExecEvalFieldSelect implements field selection from composite types (records/tuples) in PostgreSQL's expression evaluation system. It handles the extraction of individual fields from composite values, supporting both optimized expanded record representations and standard heap tuple formats.

The function includes comprehensive validation including type compatibility checks (important after ALTER COLUMN TYPE operations), dropped column handling, and boundary checking for field numbers. It provides optimized access paths for expanded records while maintaining compatibility with standard tuple representations.

The implementation includes special handling for NULL records (returns NULL), dropped columns (returns NULL), and type mismatches (throws errors). Field numbering follows PostgreSQL conventions where user columns start at 1, and system columns are not supported.

## Parameters / Member Variables
- `state`: ExprState containing the overall expression evaluation context (currently unused)
- `econtext`: ExprContext providing runtime evaluation context (currently unused)  
- `op`: ExprEvalStep containing the specific operation data including:
  - `op->d.fieldselect.fieldnum`: AttrNumber indicating which field to extract (1-based)
  - `op->d.fieldselect.resulttype`: Expected OID of the result type for validation
  - `op->d.fieldselect.rowcache`: Cache for tuple descriptor lookups
  - `op->resvalue`: Pointer to the input record and storage for result value
  - `op->resnull`: Pointer to NULL flags for input and result

## Dependencies
- Functions called/Symbols referenced:
  - VARATT_IS_EXTERNAL_EXPANDED: Macro to detect expanded record format
  - DatumGetEOHP: Extracts expanded object header pointer
  - expanded_record_get_tupdesc: Gets tuple descriptor from expanded record
  - expanded_record_get_field: Extracts field from expanded record
  - DatumGetHeapTupleHeader: Converts Datum to HeapTupleHeader
  - HeapTupleHeaderGetTypeId: Extracts type OID from tuple header
  - HeapTupleHeaderGetTypMod: Extracts type modifier from tuple header
  - [get_cached_rowtype](../g/get_cached_rowtype.md): Retrieves cached tuple descriptor for type
  - HeapTupleHeaderGetDatumLength: Gets tuple length for HeapTuple construction
  - [heap_getattr](../h/heap_getattr.md): Extracts attribute from standard heap tuple
- Called from (representative examples):
  - [ExecInterpExpr](ExecInterpExpr.md): Main expression interpreter dispatch function
  - [FunctionReturningBool](../F/FunctionReturningBool.md): JIT compilation context

## Notes and Other Information
- Supports both expanded records (optimized format) and standard heap tuples
- Includes type safety checking to detect schema changes after ALTER COLUMN TYPE
- Does not support system columns (negative field numbers) for security and consistency
- Uses caching mechanisms for tuple descriptor lookups to improve performance
- Part of PostgreSQL's compiled expression evaluation system
- Dropped columns are handled gracefully by returning NULL rather than errors
- Field numbering is 1-based following PostgreSQL attribute numbering conventions