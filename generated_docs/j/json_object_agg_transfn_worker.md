# json_object_agg_transfn_worker

## Location
[src/backend/utils/adt/json.c:993-1140](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L993-L1140)

## Overview
This function is the core worker implementation for the json_object_agg PostgreSQL aggregate function, building JSON objects by accumulating key-value pairs with optional duplicate key checking and null value handling.

## Definition
```c
static Datum json_object_agg_transfn_worker(FunctionCallInfo fcinfo, bool absent_on_null, bool unique_keys)
```

## Detailed Description
The function implements the transition logic for PostgreSQL's json_object_agg aggregate function. It accumulates input key-value pairs into a JSON object representation stored in a StringInfo buffer. The function handles several important aspects:

1. **State Management**: On first call, initializes JsonAggState containing the output buffer and optional unique key checking infrastructure
2. **Type Resolution**: Determines output functions for both key and value data types using json_categorize_type
3. **Duplicate Key Detection**: When unique_keys is enabled, maintains a hash table to detect and reject duplicate keys
4. **Null Value Handling**: When absent_on_null is true, skips entries with null values entirely
5. **JSON Formatting**: Properly formats output with comma delimiters and colon separators

The function operates within PostgreSQL's aggregate framework and ensures proper memory context management for persistent state across aggregate calls.

## Parameters / Member Variables
- `fcinfo`: PostgreSQL function call information containing arguments and context
- `absent_on_null`: Boolean flag indicating whether to skip entries with null values
- `unique_keys`: Boolean flag indicating whether to enforce key uniqueness

## Dependencies
- Functions called/Symbols referenced:
  - [AggCheckCallContext](../A/AggCheckCallContext.md)
  - [makeStringInfo](../m/makeStringInfo.md)
  - [json_unique_builder_init](json_unique_builder_init.md)
  - [get_fn_expr_argtype](../g/get_fn_expr_argtype.md)
  - [json_categorize_type](json_categorize_type.md)
  - [json_unique_builder_get_throwawaybuf](json_unique_builder_get_throwawaybuf.md)
  - [datum_to_json_internal](../d/datum_to_json_internal.md)
  - [MemoryContextStrdup](../M/MemoryContextStrdup.md)
  - [json_unique_check_key](json_unique_check_key.md)
- Data structures used:
  - [JsonAggState](../J/JsonAggState.md)
  - [FunctionCallInfo](../F/FunctionCallInfo.md)
  - StringInfo
  - [MemoryContext](../M/MemoryContext.md)
- Called from (representative examples):
  - [json_object_agg_transfn](json_object_agg_transfn.md)
  - [json_object_agg_strict_transfn](json_object_agg_strict_transfn.md)
  - [json_object_agg_unique_transfn](json_object_agg_unique_transfn.md)
  - [json_object_agg_unique_strict_transfn](json_object_agg_unique_strict_transfn.md)

## Notes and Other Information
- This is a static function, only accessible within the json.c compilation unit
- Handles PostgreSQL's "any" type arguments, including UNKNOWN types
- Enforces non-null keys as required by JSON specification
- Uses memory context switching to ensure aggregate state persists across calls
- Implements performance optimizations for null value skipping and duplicate key detection
- Returns ERROR for duplicate keys when unique_keys is enabled
- Part of PostgreSQL's SQL standard JSON aggregate function implementation

## Simplified Source

```c
static Datum
json_object_agg_transfn_worker(FunctionCallInfo fcinfo,
                               bool absent_on_null, bool unique_keys)
{
    MemoryContext aggcontext, oldcontext;
    JsonAggState *state;
    StringInfo out;
    Datum arg;
    bool skip;
    int key_offset;

    // Validate aggregate context
    if (!AggCheckCallContext(fcinfo, &aggcontext))
        elog(ERROR, "json_object_agg_transfn called in non-aggregate context");

    // Initialize state on first call
    if (PG_ARGISNULL(0))
    {
        Oid arg_type;

        // Create persistent state in aggregate memory context
        oldcontext = MemoryContextSwitchTo(aggcontext);
        state = (JsonAggState *) palloc(sizeof(JsonAggState));
        state->str = makeStringInfo();

        // Initialize unique key checking if requested
        if (unique_keys)
            json_unique_builder_init(&state->unique_check);
        else
            memset(&state->unique_check, 0, sizeof(state->unique_check));
        MemoryContextSwitchTo(oldcontext);

        // Categorize key type (argument 1)
        arg_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
        if (arg_type == InvalidOid)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                           errmsg("could not determine data type for argument %d", 1)));
        json_categorize_type(arg_type, false, &state->key_category,
                            &state->key_output_func);

        // Categorize value type (argument 2)
        arg_type = get_fn_expr_argtype(fcinfo->flinfo, 2);
        if (arg_type == InvalidOid)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                           errmsg("could not determine data type for argument %d", 2)));
        json_categorize_type(arg_type, false, &state->val_category,
                            &state->val_output_func);

        // Start JSON object
        appendStringInfoString(state->str, "{ ");
    }
    else
    {
        state = (JsonAggState *) PG_GETARG_POINTER(0);
    }

    // Keys cannot be null
    if (PG_ARGISNULL(1))
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                       errmsg("null value not allowed for object key")));

    // Skip null values if in strict mode
    skip = absent_on_null && PG_ARGISNULL(2);

    if (skip)
    {
        // For unique keys, still need to check key even if skipping value
        if (!unique_keys)
            PG_RETURN_POINTER(state);
        out = json_unique_builder_get_throwawaybuf(&state->unique_check);
    }
    else
    {
        out = state->str;
        // Add comma separator for non-first entries
        if (out->len > 2)
            appendStringInfoString(out, ", ");
    }

    // Convert and append key
    arg = PG_GETARG_DATUM(1);
    key_offset = out->len;
    datum_to_json_internal(arg, false, out, state->key_category,
                          state->key_output_func, true);

    // Check for duplicate keys if required
    if (unique_keys)
    {
        const char *key = MemoryContextStrdup(aggcontext,
                                             &out->data[key_offset]);
        if (!json_unique_check_key(&state->unique_check.check, key, 0))
            ereport(ERROR, (errcode(ERRCODE_DUPLICATE_JSON_OBJECT_KEY_VALUE),
                           errmsg("duplicate JSON object key value: %s", key)));

        if (skip)
            PG_RETURN_POINTER(state);
    }

    // Add colon separator and convert value
    appendStringInfoString(state->str, " : ");

    if (PG_ARGISNULL(2))
        arg = (Datum) 0;
    else
        arg = PG_GETARG_DATUM(2);

    datum_to_json_internal(arg, PG_ARGISNULL(2), state->str,
                          state->val_category, state->val_output_func, false);

    PG_RETURN_POINTER(state);
}
```