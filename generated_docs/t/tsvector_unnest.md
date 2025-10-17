# tsvector_unnest

## Location
[src/backend/utils/adt/tsvector_op.c:632-719](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_op.c#L632-L719)

## Overview
Expands a TSVector into a table with separate columns for lexemes, positions, and weights, implementing a set-returning function for detailed TSVector analysis.

## Definition
```c
Datum tsvector_unnest(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements a set-returning function (SRF) that unnests a TSVector into a tabular format with three columns: lexeme (text), positions (integer array), and weights (text array). It allows detailed inspection of TSVector contents by exposing the internal structure in a user-friendly format.

The function uses PostgreSQL's SRF framework to iterate through each lexeme in the TSVector. For each lexeme, it extracts the lexeme text, and if position information is available, it separates the combined position-weight values into distinct position and weight arrays. Positions are extracted as 14-bit values and weights are converted from internal numeric representation to character form ('A', 'B', 'C', 'D').

The function creates a tuple descriptor with three columns and processes each lexeme entry sequentially, handling cases where position information may not be present (setting positions and weights to NULL).

## Parameters / Member Variables
- `PG_FUNCTION_ARGS[0]`: Input TSVector to unnest into tabular format

## Dependencies
- Functions called/Symbols referenced:
  - SRF_IS_FIRSTCALL - Check if this is the first call of SRF
  - SRF_FIRSTCALL_INIT - [Initialize](../I/Initialize.md) SRF context  
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md) - Create tuple descriptor template
  - [TupleDescInitEntry](../T/TupleDescInitEntry.md) - [Initialize](../I/Initialize.md) tuple descriptor column
  - [get_call_result_type](../g/get_call_result_type.md) - Verify return type
  - PG_GETARG_TSVECTOR_COPY - Extract and copy TSVector argument
  - SRF_PERCALL_SETUP - Setup for each SRF call
  - ARRPTR - Get pointer to WordEntry array
  - STRPTR - Get pointer to string data
  - [cstring_to_text_with_len](../c/cstring_to_text_with_len.md) - Convert C string to PostgreSQL text
  - _POSVECPTR - Get position vector pointer
  - WEP_GETPOS - Extract position from position-weight value
  - WEP_GETWEIGHT - Extract weight from position-weight value
  - [construct_array_builtin](../c/construct_array_builtin.md) - Build PostgreSQL array
  - [heap_form_tuple](../h/heap_form_tuple.md) - Create heap tuple
  - SRF_RETURN_NEXT - Return next row in SRF
  - SRF_RETURN_DONE - Signal SRF completion
- Called from (representative examples):
  - No direct references found (called through SQL function dispatch as table function)

## Notes and Other Information
- Returns a table with columns: lexeme (text), positions (int2[]), weights (text[])
- Uses PostgreSQL's Set Returning Function (SRF) framework for row-by-row processing
- Handles lexemes without position information by setting positions and weights to NULL  
- Converts internal weight representation (0-3) to character form ('D'-'A') where D=lowest, A=highest weight
- Extracts 14-bit positions from the combined 16-bit position-weight storage format
- Useful for debugging and detailed analysis of TSVector contents
- Part of PostgreSQL's full-text search functionality for TSVector inspection

## Simplified Source

```c
Datum tsvector_unnest(PG_FUNCTION_ARGS) {
    FuncCallContext *funcctx;
    TSVector tsin;

    if (SRF_IS_FIRSTCALL()) {
        // Initialize SRF context and setup tuple descriptor
        funcctx = SRF_FIRSTCALL_INIT();
        MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        // Create tuple descriptor for 3 columns: lexeme, positions, weights
        TupleDesc tupdesc = CreateTemplateTupleDesc(3);
        TupleDescInitEntry(tupdesc, 1, "lexeme", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 2, "positions", INT2ARRAYOID, -1, 0);
        TupleDescInitEntry(tupdesc, 3, "weights", TEXTARRAYOID, -1, 0);

        // Validate return type and store input TSVector
        if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
            elog(ERROR, "return type must be a row type");
        funcctx->tuple_desc = tupdesc;
        funcctx->user_fctx = PG_GETARG_TSVECTOR_COPY(0);

        MemoryContextSwitchTo(oldcontext);
    }

    // Setup for each call and get TSVector
    funcctx = SRF_PERCALL_SETUP();
    tsin = (TSVector) funcctx->user_fctx;

    if (funcctx->call_cntr < tsin->size) {
        // Process current lexeme entry
        WordEntry *arrin = ARRPTR(tsin);
        char *data = STRPTR(tsin);
        int i = funcctx->call_cntr;

        bool nulls[] = {false, false, false};
        Datum values[3];

        // Extract lexeme text
        values[0] = PointerGetDatum(cstring_to_text_with_len(
            data + arrin[i].pos, arrin[i].len));

        if (arrin[i].haspos) {
            // Extract position and weight data
            WordEntryPosVector *posv = _POSVECPTR(tsin, arrin + i);
            Datum *positions = palloc(posv->npos * sizeof(Datum));
            Datum *weights = palloc(posv->npos * sizeof(Datum));

            // Separate combined position-weight values
            for (int j = 0; j < posv->npos; j++) {
                positions[j] = Int16GetDatum(WEP_GETPOS(posv->pos[j]));
                char weight = 'D' - WEP_GETWEIGHT(posv->pos[j]);
                weights[j] = PointerGetDatum(cstring_to_text_with_len(&weight, 1));
            }

            // Build arrays for positions and weights
            values[1] = PointerGetDatum(construct_array_builtin(positions, posv->npos, INT2OID));
            values[2] = PointerGetDatum(construct_array_builtin(weights, posv->npos, TEXTOID));
        } else {
            // No position data available
            nulls[1] = nulls[2] = true;
        }

        // Create and return tuple for this lexeme
        HeapTuple tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    } else {
        // All lexemes processed
        SRF_RETURN_DONE(funcctx);
    }
}
```