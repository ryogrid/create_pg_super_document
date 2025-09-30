# ExecEvalRowNullInt

## Location
[src/backend/executor/execExprInterp.c:2759-2844](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L2759-L2844)

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
  - [get_cached_rowtype](../g/get_cached_rowtype.md): Get cached tuple descriptor for the row type
  - HeapTupleHeaderGetDatumLength: Get the length of the tuple data
  - [heap_attisnull](../h/heap_attisnull.md): Check if a specific attribute in the tuple is null
  - TupleDescAttr: Access tuple descriptor attributes
  - [BoolGetDatum](../B/BoolGetDatum.md): Convert boolean to Datum
- Called from (representative examples):
  - [ExecEvalRowNull](ExecEvalRowNull.md): Wrapper for IS NULL testing
  - [ExecEvalRowNotNull](ExecEvalRowNotNull.md): Wrapper for IS NOT NULL testing

## Notes and Other Information
- This function is static and only used internally within the expression interpreter
- The null testing is non-recursive - it uses primitive heap_attisnull tests rather than recursive row null checks
- Dropped columns are ignored during the null testing process
- The function caches row type information in op->d.nulltest_row.rowcache for efficiency
- The implementation follows SQL standard semantics precisely and handles edge cases like zero-field rows
- Performance is optimized with early termination to avoid unnecessary field checks

## Simplified Source

```c
static void ExecEvalRowNullInt(ExprState *state, ExprEvalStep *op,
                               ExprContext *econtext, bool checkisnull) {
    Datum value = *op->resvalue;
    bool isnull = *op->resnull;
    HeapTupleHeader tuple;
    Oid tupType;
    int32 tupTypmod;
    TupleDesc tupDesc;
    HeapTupleData tmptup;

    *op->resnull = false;

    // Handle NULL row variables as NULL scalar columns
    if (isnull) {
        *op->resvalue = BoolGetDatum(checkisnull);
        return;
    }

    // Extract tuple information
    tuple = DatumGetHeapTupleHeader(value);
    tupType = HeapTupleHeaderGetTypeId(tuple);
    tupTypmod = HeapTupleHeaderGetTypMod(tuple);

    // Get cached tuple descriptor
    tupDesc = get_cached_rowtype(tupType, tupTypmod, &op->d.nulltest_row.rowcache, NULL);

    // Prepare HeapTuple for heap_attisnull calls
    tmptup.t_len = HeapTupleHeaderGetDatumLength(tuple);
    tmptup.t_data = tuple;

    // Check each attribute for null status
    for (int att = 1; att <= tupDesc->natts; att++) {
        // Skip dropped columns
        if (TupleDescAttr(tupDesc, att - 1)->attisdropped)
            continue;

        if (heap_attisnull(&tmptup, att, tupDesc)) {
            // Found null field
            if (!checkisnull) {
                // IS NOT NULL test fails when any field is null
                *op->resvalue = BoolGetDatum(false);
                return;
            }
        } else {
            // Found non-null field
            if (checkisnull) {
                // IS NULL test fails when any field is non-null
                *op->resvalue = BoolGetDatum(false);
                return;
            }
        }
    }

    // All fields passed the test
    *op->resvalue = BoolGetDatum(true);
}
```