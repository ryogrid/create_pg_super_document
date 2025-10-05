# pg_stats_ext_mcvlist_items

## Location
[src/backend/statistics/mcv.c:1338-1471](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/mcv.c#L1338-L1471)

## Overview
A Set-Returning Function (SRF) that exposes detailed information about individual items in a Most Common Values (MCV) statistics list as SQL-accessible tuples for administrative and analytical purposes.

## Definition

```c
Datum
pg_stats_ext_mcvlist_items(PG_FUNCTION_ARGS)
```
## Detailed Description
This function provides SQL access to MCV list contents by deserializing the binary statistics data and returning each MCV item as a tuple. The function operates as a set-returning function, yielding one tuple per MCV item containing:

- **Item ID**: Sequential index (0 to nitems-1)  
- **Values**: Text array of the actual values for each dimension
- **Nulls**: Boolean array indicating null status for each dimension
- **Frequency**: Observed frequency of this value combination
- **Base frequency**: Expected frequency under independence assumption

The function handles the complete SRF lifecycle, including initialization on first call, per-call tuple generation, and cleanup on completion. It converts internal Datum values to their string representations using appropriate output functions for each data type.

## Parameters / Member Variables
- Function takes a single parameter via : The serialized MCV list data as bytea

## Dependencies
- Functions called/Symbols referenced:
  -  - Deserializes input bytea to MCVList
  - // - SRF management macros
  - / - Tuple descriptor setup
  - / - Array construction utilities
  - / - Type output conversion
  - / - Tuple creation
  - / - SRF return macros

- Called from (representative examples):
  - Exposed as SQL function for querying MCV list contents
  - Used by database administrators and query analysis tools

## Notes and Other Information
- Returns composite tuples with 5 columns: item_number, values[], nulls[], frequency, base_frequency
- Handles multi-dimensional statistics by building arrays for values and null flags
- Uses proper memory context management for multi-call function persistence  
- Converts internal Datum values to text representations using type-specific output functions
- Essential for introspecting extended statistics data and understanding query planner decisions
- The function will return no rows if the input statistics contains no MCV data

## Simplified Source

```c
Datum pg_stats_ext_mcvlist_items(PG_FUNCTION_ARGS) {
    FuncCallContext *funcctx;

    // First call: initialize SRF
    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        MCVList *mcvlist;
        TupleDesc tupdesc;

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        // Deserialize MCV list
        mcvlist = statext_mcv_deserialize(PG_GETARG_BYTEA_P(0));
        funcctx->user_fctx = mcvlist;

        // Set max calls based on number of items
        funcctx->max_calls = 0;
        if (funcctx->user_fctx != NULL)
            funcctx->max_calls = mcvlist->nitems;

        // Setup tuple descriptor
        if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("function returning record called in context that cannot accept type record")));
        tupdesc = BlessTupleDesc(tupdesc);
        funcctx->attinmeta = TupleDescGetAttInMetadata(tupdesc);

        MemoryContextSwitchTo(oldcontext);
    }

    // Per-call processing
    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->call_cntr < funcctx->max_calls) {
        Datum values[5];
        bool nulls[5];
        HeapTuple tuple;
        ArrayBuildState *astate_values = NULL;
        ArrayBuildState *astate_nulls = NULL;

        MCVList *mcvlist = (MCVList *) funcctx->user_fctx;
        MCVItem *item = &mcvlist->items[funcctx->call_cntr];

        // Build arrays for values and nulls
        for (int i = 0; i < mcvlist->ndimensions; i++) {
            // Add null flag to array
            astate_nulls = accumArrayResult(astate_nulls, BoolGetDatum(item->isnull[i]),
                                          false, BOOLOID, CurrentMemoryContext);

            if (!item->isnull[i]) {
                // Convert value to text and add to array
                bool isvarlena;
                Oid outfunc;
                FmgrInfo fmgrinfo;

                getTypeOutputInfo(mcvlist->types[i], &outfunc, &isvarlena);
                fmgr_info(outfunc, &fmgrinfo);

                Datum val = FunctionCall1(&fmgrinfo, item->values[i]);
                text *txt = cstring_to_text(DatumGetPointer(val));

                astate_values = accumArrayResult(astate_values, PointerGetDatum(txt),
                                               false, TEXTOID, CurrentMemoryContext);
            } else {
                astate_values = accumArrayResult(astate_values, (Datum) 0,
                                               true, TEXTOID, CurrentMemoryContext);
            }
        }

        // Build result tuple: (item_id, values[], nulls[], frequency, base_frequency)
        values[0] = Int32GetDatum(funcctx->call_cntr);
        values[1] = makeArrayResult(astate_values, CurrentMemoryContext);
        values[2] = makeArrayResult(astate_nulls, CurrentMemoryContext);
        values[3] = Float8GetDatum(item->frequency);
        values[4] = Float8GetDatum(item->base_frequency);

        memset(nulls, 0, sizeof(nulls));

        tuple = heap_form_tuple(funcctx->attinmeta->tupdesc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    } else {
        SRF_RETURN_DONE(funcctx);
    }
}
```