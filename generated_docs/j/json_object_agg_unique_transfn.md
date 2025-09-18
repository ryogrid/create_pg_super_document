# json_object_agg_unique_transfn

## Location
src/backend/utils/adt/json.c: 1159 - 1167

## Overview
A PostgreSQL aggregate transition function that builds JSON objects from key-value pairs while enforcing key uniqueness during aggregation.

## Definition


## Detailed Description
This function serves as the transition function for the json_object_agg_unique aggregate. It is a thin wrapper around the core worker function , specifically configured to enforce unique keys in the resulting JSON object. The function processes each key-value pair input during aggregation and ensures that duplicate keys are not allowed, throwing an error if a duplicate key is encountered.

The function delegates all the actual work to  with the parameters  and , meaning it will include NULL values in the output but will strictly enforce key uniqueness.

## Parameters / Member Variables
- Uses PostgreSQL's standard  macro for function arguments
- Argument 0: Internal aggregate state (JsonAggState pointer)
- Argument 1: Key value (any type, converted to JSON string)
- Argument 2: Value to associate with the key (any type, converted to JSON)

## Dependencies
- Functions called/Symbols referenced:
  - json_object_agg_transfn_worker
- Called from (representative examples):
  - PostgreSQL aggregate framework (no direct callers in source)

## Notes and Other Information
- This is an internal function used by PostgreSQL's aggregate system
- Cannot be called directly due to internal-type arguments
- Part of the JSON object aggregation functionality introduced for building JSON objects from query results
- The unique key enforcement helps prevent malformed JSON objects with duplicate keys
- Located in src/backend/utils/adt/json.c:1159-1167