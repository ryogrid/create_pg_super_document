# jsonb_object_agg_transfn_worker

## Location
[src/backend/utils/adt/jsonb.c:1673-1895](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L1673-L1895)

## Overview
Core worker function that implements the transition logic for JSONB object aggregation, handling key-value pair accumulation with configurable null handling and key uniqueness policies.

## Definition

```c
structure for key");
```
## Detailed Description
This is the comprehensive worker function that powers all JSONB object aggregation variants. It accumulates key-value pairs into a JSONB object during aggregate processing. The function handles initialization of the aggregate state on first call, validates key types (must be strings), processes both keys and values through the JSONB conversion pipeline, and maintains the growing object structure. It supports configurable behavior for NULL value handling and key uniqueness validation through its boolean parameters.

The function operates in two main phases: state initialization (first call) where it sets up JsonbAggState and determines input data types, and accumulation phase where it processes each key-value pair. Keys must be convertible to strings, while values can be any type including complex nested structures.

## Parameters / Member Variables
- : Function call information containing aggregate state and input key/value arguments
- : If true, skip key-value pairs where the value is NULL (unless unique_keys is true)
- : If true, enforce unique key constraints in the resulting object

## Dependencies
- Functions called/Symbols referenced:
  - [JsonbInState](../J/JsonbInState.md), JsonbAggState, JsonbIterator, Jsonb
  - [AggCheckCallContext](../A/AggCheckCallContext.md), MemoryContextSwitchTo
  - [pushJsonbValue](../p/pushJsonbValue.md), JsonbIteratorInit, JsonbIteratorNext
  - [datum_to_jsonb_internal](../d/datum_to_jsonb_internal.md), JsonbValueToJsonb
  - [get_fn_expr_argtype](../g/get_fn_expr_argtype.md), json_categorize_type
  - WJB_BEGIN_OBJECT, WJB_KEY, WJB_VALUE, WJB_END_ARRAY, etc.
- Called from (representative examples):
  - [jsonb_object_agg_transfn](jsonb_object_agg_transfn.md)
  - [jsonb_object_agg_strict_transfn](jsonb_object_agg_strict_transfn.md)
  - [jsonb_object_agg_unique_transfn](jsonb_object_agg_unique_transfn.md)
  - [jsonb_object_agg_unique_strict_transfn](jsonb_object_agg_unique_strict_transfn.md)

## Notes and Other Information
- Static function, only accessible within the same compilation unit
- Enforces that object keys must be strings, raising errors for other types
- Handles memory context switching for proper aggregate context management
- Supports both simple scalar values and complex nested JSON structures as values
- Implements string and numeric value copying to ensure proper memory management
- Part of PostgreSQL's JSONB aggregate function family located in src/backend/utils/adt/jsonb.c:1673-1895

## Simplified Source

```c
static Datum
jsonb_object_agg_transfn_worker(FunctionCallInfo fcinfo,
                               bool absent_on_null, bool unique_keys)
{
    MemoryContext oldcontext, aggcontext;
    JsonbAggState *state;
    JsonbInState *result;
    JsonbInState elem;
    Datum val;
    Jsonb *jbkey, *jbval;
    JsonbValue v;
    JsonbIterator *it;
    JsonbIteratorToken type;
    bool skip, single_scalar;

    // Initialize aggregate state on first call
    if (PG_ARGISNULL(0)) {
        oldcontext = MemoryContextSwitchTo(aggcontext);

        // Create new aggregate state
        state = palloc(sizeof(JsonbAggState));
        result = palloc0(sizeof(JsonbInState));
        state->res = result;

        // Begin JSONB object construction
        result->res = pushJsonbValue(&result->parseState, WJB_BEGIN_OBJECT, NULL);
        result->parseState->unique_keys = unique_keys;
        result->parseState->skip_nulls = absent_on_null;

        // Determine input data types for key and value
        json_categorize_type(get_fn_expr_argtype(fcinfo->flinfo, 1),
                           true, &state->key_category, &state->key_output_func);
        json_categorize_type(get_fn_expr_argtype(fcinfo->flinfo, 2),
                           true, &state->val_category, &state->val_output_func);

        MemoryContextSwitchTo(oldcontext);
    } else {
        state = (JsonbAggState *) PG_GETARG_POINTER(0);
        result = state->res;
    }

    // Keys cannot be NULL
    if (PG_ARGISNULL(1))
        ereport(ERROR, (errmsg("field name must not be null")));

    // Skip null values if configured (unless uniqueness check needed)
    skip = absent_on_null && PG_ARGISNULL(2);
    if (skip && !unique_keys)
        PG_RETURN_POINTER(state);

    // Convert key to JSONB and validate it's a string
    val = PG_GETARG_DATUM(1);
    memset(&elem, 0, sizeof(JsonbInState));
    datum_to_jsonb_internal(val, false, &elem, state->key_category,
                           state->key_output_func, true);
    jbkey = JsonbValueToJsonb(elem.res);

    // Convert value to JSONB
    val = PG_ARGISNULL(2) ? (Datum) 0 : PG_GETARG_DATUM(2);
    memset(&elem, 0, sizeof(JsonbInState));
    datum_to_jsonb_internal(val, PG_ARGISNULL(2), &elem, state->val_category,
                           state->val_output_func, false);
    jbval = JsonbValueToJsonb(elem.res);

    oldcontext = MemoryContextSwitchTo(aggcontext);

    // Process key - must be a string
    it = JsonbIteratorInit(&jbkey->root);
    while ((type = JsonbIteratorNext(&it, &v, false)) != WJB_DONE) {
        if (type == WJB_ELEM) {
            if (v.type != jbvString)
                ereport(ERROR, (errmsg("object keys must be strings")));

            // Copy string value to aggregate context
            char *buf = palloc(v.val.string.len + 1);
            snprintf(buf, v.val.string.len + 1, "%s", v.val.string.val);
            v.val.string.val = buf;

            // Add key to object
            result->res = pushJsonbValue(&result->parseState, WJB_KEY, &v);

            // Handle null value case
            if (skip) {
                v.type = jbvNull;
                result->res = pushJsonbValue(&result->parseState, WJB_VALUE, &v);
                MemoryContextSwitchTo(oldcontext);
                PG_RETURN_POINTER(state);
            }
        }
    }

    // Process value - can be any JSONB structure
    it = JsonbIteratorInit(&jbval->root);
    single_scalar = false;

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
                // Copy string and numeric values to aggregate context
                if (v.type == jbvString) {
                    char *buf = palloc(v.val.string.len + 1);
                    snprintf(buf, v.val.string.len + 1, "%s", v.val.string.val);
                    v.val.string.val = buf;
                } else if (v.type == jbvNumeric) {
                    v.val.numeric = DatumGetNumeric(DirectFunctionCall1(numeric_uplus,
                                                   NumericGetDatum(v.val.numeric)));
                }

                result->res = pushJsonbValue(&result->parseState,
                                           single_scalar ? WJB_VALUE : type, &v);
                break;
        }
    }

    MemoryContextSwitchTo(oldcontext);
    PG_RETURN_POINTER(state);
}
```