# ExecEvalRowNullInt

## Location
src/backend/executor/execExprInterp.c: 2759 - 2844

## Overview
ExecEvalRowNullInt implements the core logic for evaluating IS NULL and IS NOT NULL tests on row expressions, checking whether all or any fields in a composite type are null.

## Definition
```c
static void ExecEvalRowNullInt(ExprState *state, ExprEvalStep *op, ExprContext *econtext, bool checkisnull)
```

## Detailed Description
This function implements the SQL standard definition for null testing on row types. According to the SQL standard:
- "R IS NULL" is true if every field is the null value
- "R IS NOT NULL" is true if no field is the null value

The function first handles the case where the entire row variable is NULL (treated as a NULL scalar). For non-null row values, it extracts the tuple header, gets the tuple descriptor, and iterates through all non-dropped attributes to check their null status using heap_attisnull.

The function uses early termination logic:
- For IS NULL tests: returns false as soon as any non-null field is found
- For IS NOT NULL tests: returns false as soon as any null field is found
- If all fields are checked without early termination, returns true

Zero-field rows vacuously satisfy both predicates according to this implementation.

## Parameters / Member Variables
- `state`: ExprState pointer containing the expression evaluation state
- `op`: ExprEvalStep pointer containing operation details and cached row type information
- `econtext`: ExprContext pointer providing the expression evaluation context
- `checkisnull`: Boolean flag indicating whether to test for IS NULL (true) or IS NOT NULL (false)

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetHeapTupleHeader: Extract tuple header from Datum
  - HeapTupleHeaderGetTypeId: Get the type OID from tuple header
  - HeapTupleHeaderGetTypMod: Get the type modifier from tuple header
  - get_cached_rowtype: Get cached tuple descriptor for the row type
  - HeapTupleHeaderGetDatumLength: Get the length of the tuple data
  - heap_attisnull: Check if a specific attribute in the tuple is null
  - TupleDescAttr: Access tuple descriptor attributes
  - BoolGetDatum: Convert boolean to Datum
- Called from (representative examples):
  - ExecEvalRowNull: Wrapper for IS NULL testing
  - ExecEvalRowNotNull: Wrapper for IS NOT NULL testing

## Notes and Other Information
- This function is static and only used internally within the expression interpreter
- The null testing is non-recursive - it uses primitive heap_attisnull tests rather than recursive row null checks
- Dropped columns are ignored during the null testing process
- The function caches row type information in op->d.nulltest_row.rowcache for efficiency
- The implementation follows SQL standard semantics precisely and handles edge cases like zero-field rows
- Performance is optimized with early termination to avoid unnecessary field checks