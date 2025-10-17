# jsonb_path_query_internal

## Location
[src/backend/utils/adt/jsonpath_exec.c:526-573](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L526-L573)

## Overview
Internal implementation function that executes a JSONPath expression against a JSONB document and returns matching values as a set-returning function (rowset).

## Definition

```c
static Datum
jsonb_path_query_internal(FunctionCallInfo fcinfo, bool tz)
```
## Detailed Description
`jsonb_path_query_internal` is the core implementation function for JSONPath query operations that return multiple results. Unlike the predicate matching functions, this function executes a JSONPath expression and returns all matching values as a rowset using PostgreSQL's set-returning function (SRF) mechanism.

The function operates in two phases:
1. **First call (SRF_IS_FIRSTCALL)**: Executes the JSONPath expression against the JSONB document, collecting all matching values into a list stored in the function context.
2. **Subsequent calls (SRF_PERCALL_SETUP)**: Returns one matching value per call until all results are exhausted.

The function supports timezone-aware datetime operations when the `tz` parameter is true, making it suitable for both timezone-aware and timezone-unaware query operations.

## Parameters / Member Variables
- `fcinfo`: Function call information containing:
- `tz`: Boolean flag indicating whether to enable timezone-aware datetime operations

## Dependencies
- Functions called/Symbols referenced:
  - [executeJsonPath](../e/executeJsonPath.md)
  - [getJsonPathVariableFromJsonb](../g/getJsonPathVariableFromJsonb.md)
  - [countVariablesFromJsonb](../c/countVariablesFromJsonb.md)
  - [JsonValueListGetList](../J/JsonValueListGetList.md)
  - [JsonbValueToJsonb](../J/JsonbValueToJsonb.md)
  - [JsonbPGetDatum](../J/JsonbPGetDatum.md)
  - SRF_FIRSTCALL_INIT, SRF_PERCALL_SETUP, SRF_RETURN_NEXT, SRF_RETURN_DONE
  - [list_head](../l/list_head.md), list_delete_first
  - PG_GETARG_JSONB_P_COPY, PG_GETARG_JSONPATH_P_COPY, PG_GETARG_BOOL
- Called from (representative examples):
  - [jsonb_path_query](jsonb_path_query.md)
  - [jsonb_path_query_tz](jsonb_path_query_tz.md)

## Notes and Other Information
- Implements PostgreSQL's set-returning function protocol for returning multiple results
- Uses memory context switching to ensure proper memory management across multiple calls
- Supports variable substitution in JSONPath expressions
- Silent mode controls error handling behavior during path evaluation
- Returns each matching JSONB value as a separate row in the result set
- Located in `src/backend/utils/adt/jsonpath_exec.c:526-573`

## Simplified Source

```c
static Datum jsonb_path_query_internal(FunctionCallInfo fcinfo, bool tz) {
    FuncCallContext *funcctx;
    List *found;
    JsonbValue *v;
    ListCell *c;

    if (SRF_IS_FIRSTCALL()) {
        // First call: setup and execute JSONPath query
        JsonPath *jp;
        Jsonb *jb;
        MemoryContext oldcontext;
        JsonValueList found_values = {0};

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        // Extract function arguments
        jb = PG_GETARG_JSONB_P_COPY(0);       // JSONB document
        jp = PG_GETARG_JSONPATH_P_COPY(1);    // JSONPath expression
        Jsonb *vars = PG_GETARG_JSONB_P_COPY(2);  // Variables
        bool silent = PG_GETARG_BOOL(3);     // Silent mode flag

        // Execute JSONPath and collect all matching values
        executeJsonPath(jp, vars, getJsonPathVariableFromJsonb,
                       countVariablesFromJsonb, jb, !silent, &found_values, tz);

        // Store results in function context for subsequent calls
        funcctx->user_fctx = JsonValueListGetList(&found_values);
        MemoryContextSwitchTo(oldcontext);
    }

    // Subsequent calls: return one result per call
    funcctx = SRF_PERCALL_SETUP();
    found = funcctx->user_fctx;

    c = list_head(found);
    if (c == NULL)
        SRF_RETURN_DONE(funcctx);  // No more results

    // Return next matching value and remove from list
    v = lfirst(c);
    funcctx->user_fctx = list_delete_first(found);

    SRF_RETURN_NEXT(funcctx, JsonbPGetDatum(JsonbValueToJsonb(v)));
}
```