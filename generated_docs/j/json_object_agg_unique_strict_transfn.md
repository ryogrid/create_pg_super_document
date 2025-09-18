# json_object_agg_unique_strict_transfn

## Location
[src/backend/utils/adt/json.c:1168-1176](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L1168-L1176)

## Overview
A PostgreSQL aggregate transition function that builds JSON objects from key-value pairs while enforcing key uniqueness and omitting NULL values during aggregation.

## Definition
```c
Datum json_object_agg_unique_strict_transfn(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the transition function for the json_object_agg_unique_strict aggregate. It is a wrapper around the core worker function `json_object_agg_transfn_worker`, specifically configured to both enforce unique keys and exclude NULL values from the resulting JSON object. The function processes each key-value pair input during aggregation, ensuring that duplicate keys are not allowed and that entries with NULL values are absent from the final output.

The function delegates all the actual work to `json_object_agg_transfn_worker` with the parameters `absent_on_null=true` and `unique_keys=true`, meaning it will skip NULL values in the output and strictly enforce key uniqueness.

## Parameters / Member Variables
- Uses PostgreSQL's standard `PG_FUNCTION_ARGS` macro for function arguments
- Argument 0: Internal aggregate state (JsonAggState pointer)
- Argument 1: Key value (any type, converted to JSON string)
- Argument 2: Value to associate with the key (any type, converted to JSON)

## Dependencies
- Functions called/Symbols referenced:
  - [json_object_agg_transfn_worker](json_object_agg_transfn_worker.md)
- Called from (representative examples):
  - PostgreSQL aggregate framework (no direct callers in source)

## Notes and Other Information
- This is an internal function used by PostgreSQL's aggregate system
- Cannot be called directly due to internal-type arguments
- Part of the JSON object aggregation functionality for building JSON objects from query results
- The 'strict' behavior means NULL values are omitted from the output JSON object
- The unique key enforcement prevents malformed JSON objects with duplicate keys
- Located in src/backend/utils/adt/json.c:1168-1176