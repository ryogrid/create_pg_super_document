# jsonb_agg_transfn_worker

## Location
[src/backend/utils/adt/jsonb.c:1501-1624](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L1501-L1624)

## Overview
Worker function that implements the core logic for JSONB array aggregation transition functions, handling both standard and strict (absent-on-null) variants.

## Definition

```c
static Datum
jsonb_agg_transfn_worker(FunctionCallInfo fcinfo, bool absent_on_null)
```
## Detailed Description
The  function serves as the core implementation for JSONB array aggregation transition functions. It accumulates individual values into a JSONB array during aggregate processing. The function handles the initialization of the aggregate state on first call, converts input values to JSONB format, and iterates through the JSONB structure to properly integrate elements into the growing array. It supports both standard aggregation and strict mode (where null inputs are skipped when absent_on_null is true).

## Parameters / Member Variables
- `fcinfo`: Function call information containing arguments and context
- `absent_on_null`: Boolean flag indicating whether to skip null values (true for strict aggregation)
## Dependencies
- Functions called/Symbols referenced:
  -  - Verify aggregate function context
  -  - Get argument type information
  - ,  - Memory allocation functions
  -  - Add values to JSONB structure
  -  - Determine JSON type category
  -  - Convert datum to JSONB
  -  - Convert JsonbValue to final JSONB
  - ,  - JSONB iteration functions
  - ,  - [Numeric](../N/Numeric.md) value copying
  - Memory context functions: 
  - Constants: , , , , , , , 
- Called from:
  -  (src/backend/utils/adt/jsonb.c:1627)
  -  (src/backend/utils/adt/jsonb.c:1636)

## Notes and Other Information
- Handles both initialization (first call with null state) and accumulation phases
- Manages memory contexts properly for aggregate operations
- Special handling for scalar arrays to avoid double-wrapping
- Copies string and numeric values into the aggregate memory context for persistence
- Validates that it's called within proper aggregate context
- Supports conditional null handling based on absent_on_null parameter
- Uses JsonbIterator to traverse complex JSONB structures element by element

## Simplified Source

```c
static Datum
jsonb_agg_transfn_worker(FunctionCallInfo fcinfo, bool absent_on_null)
{
    MemoryContext oldcontext, aggcontext;
    JsonbAggState *state;
    JsonbInState elem;
    Datum val;
    JsonbInState *result;
    bool single_scalar = false;
    JsonbIterator *it;
    Jsonb *jbelem;
    JsonbValue v;
    JsonbIteratorToken type;

    // Validate aggregate context
    if (!AggCheckCallContext(fcinfo, &aggcontext))
        elog(ERROR, "jsonb_agg_transfn called in non-aggregate context");

    // Initialize state on first call
    if (PG_ARGISNULL(0)) {
        Oid arg_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
        if (arg_type == InvalidOid)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                           errmsg("could not determine input data type")));

        // Set up aggregate state
        oldcontext = MemoryContextSwitchTo(aggcontext);
        state = palloc(sizeof(JsonbAggState));
        result = palloc0(sizeof(JsonbInState));
        state->res = result;
        result->res = pushJsonbValue(&result->parseState, WJB_BEGIN_ARRAY, NULL);
        MemoryContextSwitchTo(oldcontext);

        json_categorize_type(arg_type, true, &state->val_category, &state->val_output_func);
    } else {
        state = (JsonbAggState *) PG_GETARG_POINTER(0);
        result = state->res;
    }

    // Skip null values if absent_on_null is true
    if (absent_on_null && PG_ARGISNULL(1))
        PG_RETURN_POINTER(state);

    // Convert input value to JSONB
    val = PG_ARGISNULL(1) ? (Datum) 0 : PG_GETARG_DATUM(1);
    memset(&elem, 0, sizeof(JsonbInState));
    datum_to_jsonb_internal(val, PG_ARGISNULL(1), &elem,
                           state->val_category, state->val_output_func, false);
    jbelem = JsonbValueToJsonb(elem.res);

    // Switch to aggregate context for accumulation
    oldcontext = MemoryContextSwitchTo(aggcontext);
    it = JsonbIteratorInit(&jbelem->root);

    // Iterate through JSONB structure and add to result array
    while ((type = JsonbIteratorNext(&it, &v, false)) != WJB_DONE) {
        switch (type) {
            case WJB_BEGIN_ARRAY:
                if (v.val.array.rawScalar)
                    single_scalar = true;
                else
                    result->res = pushJsonbValue(&result->parseState, type, NULL);
                break;
            case WJB_END_ARRAY:
                if (!single_scalar)
                    result->res = pushJsonbValue(&result->parseState, type, NULL);
                break;
            case WJB_BEGIN_OBJECT:
            case WJB_END_OBJECT:
                result->res = pushJsonbValue(&result->parseState, type, NULL);
                break;
            case WJB_ELEM:
            case WJB_KEY:
            case WJB_VALUE:
                // Copy strings and numerics into aggregate context
                if (v.type == jbvString) {
                    char *buf = palloc(v.val.string.len + 1);
                    snprintf(buf, v.val.string.len + 1, "%s", v.val.string.val);
                    v.val.string.val = buf;
                } else if (v.type == jbvNumeric) {
                    v.val.numeric = DatumGetNumeric(DirectFunctionCall1(numeric_uplus,
                                                   NumericGetDatum(v.val.numeric)));
                }
                result->res = pushJsonbValue(&result->parseState, type, &v);
                break;
            default:
                elog(ERROR, "unknown jsonb iterator token type");
        }
    }

    MemoryContextSwitchTo(oldcontext);
    PG_RETURN_POINTER(state);
}
```