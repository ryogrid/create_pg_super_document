# ExecEvalFieldStoreForm

## Location
src/backend/executor/execExprInterp.c: 3348 - 3371

## Overview
ExecEvalFieldStoreForm constructs a new composite datum (tuple) after individual field values of a FieldStore expression have been evaluated during expression execution.

## Definition
```c
void ExecEvalFieldStoreForm(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
```

## Detailed Description
This function is the final step in evaluating a FieldStore expression, which modifies specific fields within a composite type (record/row). After all individual field values have been computed and stored in the operation's temporary arrays, this function assembles them into a new HeapTuple representing the complete modified composite value.

The function retrieves the cached tuple descriptor for the result type, then uses heap_form_tuple() to construct a new tuple from the pre-computed field values and null indicators. This newly formed tuple becomes the result of the FieldStore operation.

## Parameters / Member Variables
- `state`: Expression state context (unused in this function)
- `op`: Expression evaluation step containing fieldstore operation data and result storage
- `econtext`: Expression context (unused in this function)

## Dependencies
- Functions called/Symbols referenced:
  - get_cached_rowtype: Retrieves the cached tuple descriptor for the result type
  - heap_form_tuple: Creates a new HeapTuple from field values and null indicators
  - HeapTupleGetDatum: Converts the HeapTuple to a Datum for storage
  - ExprEvalStep: Structure containing operation data and temporary storage
- Called from (representative examples):
  - ExecInterpExpr: Main expression interpreter dispatch function
  - FunctionReturningBool: JIT compilation type mapping function

## Notes and Other Information
- This function assumes that all field values and null indicators have been pre-computed and stored in op->d.fieldstore.values and op->d.fieldstore.nulls arrays
- The tuple descriptor is assumed to be valid and cached from a previous lookup
- The function always sets the result as non-null since a composite value is being constructed
- Part of PostgreSQL's expression evaluation framework for handling composite type field modifications