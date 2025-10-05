# json_agg_transfn_worker

## Location
[src/backend/utils/adt/json.c:770-851](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L770-L851)

## Overview
The json_agg_transfn_worker function implements the core transition logic for PostgreSQL's json_agg aggregate function, building a JSON array from input values.

## Definition
```c
static Datum json_agg_transfn_worker(FunctionCallInfo fcinfo, bool absent_on_null)
```

## Detailed Description
This function serves as the workhorse for JSON aggregation operations, implementing the state transition logic that accumulates input values into a JSON array format. It manages a JsonAggState structure that maintains the growing JSON array string and type information. The function handles initialization of the aggregate state, proper comma separation between array elements, null value processing, and formatting for complex types. It supports both regular and strict modes via the absent_on_null parameter, where strict mode skips null values entirely.

## Parameters / Member Variables
- `fcinfo`: FunctionCallInfo containing the aggregate context and arguments
- `absent_on_null`: Boolean flag controlling null handling behavior (true for strict mode)

## Dependencies
- Functions called/Symbols referenced:
  - [AggCheckCallContext](../A/AggCheckCallContext.md) (to validate aggregate execution context)
  - [get_fn_expr_argtype](../g/get_fn_expr_argtype.md) (to determine input data type)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (for memory management in aggregate context)
  - [makeStringInfo](../m/makeStringInfo.md) (to create the JSON array buffer)
  - [json_categorize_type](json_categorize_type.md) (to categorize input type for JSON conversion)
  - [datum_to_json_internal](../d/datum_to_json_internal.md) (to convert individual values to JSON)
- Called from:
  - [json_agg_transfn](json_agg_transfn.md) (standard json_agg aggregate function)
  - [json_agg_strict_transfn](json_agg_strict_transfn.md) (strict variant that skips nulls)

## Notes and Other Information
- Maintains state across aggregate calls using JsonAggState structure
- Handles memory context switching to ensure state persists for the aggregate duration
- Adds proper formatting including commas between elements and whitespace for structured types
- Located in src/backend/utils/adt/json.c:770-851
- Uses 'internal' transition type for efficient state passing through PostgreSQL's aggregate machinery
- Supports both null-preserving and null-skipping modes of operation

## Simplified Source

```c
static Datum
json_agg_transfn_worker(FunctionCallInfo fcinfo, bool absent_on_null)
{
    MemoryContext aggcontext, oldcontext;
    JsonAggState *state;
    Datum val;

    // Validate we're called in aggregate context
    if (!AggCheckCallContext(fcinfo, &aggcontext))
        elog(ERROR, "json_agg_transfn called in non-aggregate context");

    // Initialize state on first call (when state is NULL)
    if (PG_ARGISNULL(0))
    {
        Oid arg_type = get_fn_expr_argtype(fcinfo->flinfo, 1);

        if (arg_type == InvalidOid)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                           errmsg("could not determine input data type")));

        // Create persistent state in aggregate memory context
        oldcontext = MemoryContextSwitchTo(aggcontext);
        state = (JsonAggState *) palloc(sizeof(JsonAggState));
        state->str = makeStringInfo();
        MemoryContextSwitchTo(oldcontext);

        // Start JSON array and categorize input type
        appendStringInfoChar(state->str, '[');
        json_categorize_type(arg_type, false, &state->val_category,
                            &state->val_output_func);
    }
    else
    {
        state = (JsonAggState *) PG_GETARG_POINTER(0);
    }

    // Skip null values if in strict mode
    if (absent_on_null && PG_ARGISNULL(1))
        PG_RETURN_POINTER(state);

    // Add comma separator for non-first elements
    if (state->str->len > 1)
        appendStringInfoString(state->str, ", ");

    // Handle null values
    if (PG_ARGISNULL(1))
    {
        datum_to_json_internal((Datum) 0, true, state->str, JSONTYPE_NULL,
                              InvalidOid, false);
        PG_RETURN_POINTER(state);
    }

    val = PG_GETARG_DATUM(1);

    // Add formatting for structured types
    if (!PG_ARGISNULL(0) && state->str->len > 1 &&
        (state->val_category == JSONTYPE_ARRAY ||
         state->val_category == JSONTYPE_COMPOSITE))
    {
        appendStringInfoString(state->str, "\n ");
    }

    // Convert value to JSON and append to array
    datum_to_json_internal(val, false, state->str, state->val_category,
                          state->val_output_func, false);

    PG_RETURN_POINTER(state);
}
```