# json_object_agg_finalfn

## Location
src/backend/utils/adt/json.c: 1177 - 1199

## Overview
A PostgreSQL aggregate finalization function that completes the construction of a JSON object by closing the object bracket and returning the final result.

## Definition
```c
Datum json_object_agg_finalfn(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the finalization function for PostgreSQL's json_object_agg aggregates. It takes the accumulated state from the transition phase and produces the final JSON object result. The function handles the completion of JSON object construction by appending the closing brace and converting the accumulated StringInfo buffer into a PostgreSQL text value.

The function implements standard aggregate behavior by returning NULL when no rows were processed (state is NULL), and otherwise returns the completed JSON object string. It uses `catenate_stringinfo_string` to efficiently append the closing brace " }" to the accumulated JSON string.

## Parameters / Member Variables
- Uses PostgreSQL's standard `PG_FUNCTION_ARGS` macro for function arguments
- Argument 0: Internal aggregate state (JsonAggState pointer containing accumulated JSON string)

## Dependencies
- Functions called/Symbols referenced:
  - [AggCheckCallContext](../A/AggCheckCallContext.md)
  - [catenate_stringinfo_string](../c/catenate_stringinfo_string.md)
  - PG_RETURN_TEXT_P
- Types referenced:
  - [JsonAggState](../J/JsonAggState.md)
- Called from (representative examples):
  - PostgreSQL aggregate framework (no direct callers in source)

## Notes and Other Information
- This is an internal function used by PostgreSQL's aggregate system
- Cannot be called directly due to internal-type arguments, verified by Assert
- Returns NULL for empty result sets, following standard aggregate conventions
- Part of the JSON object aggregation functionality for building JSON objects from query results
- The function assumes the state string already contains a properly formatted JSON object beginning (with opening brace)
- Located in src/backend/utils/adt/json.c:1177-1199