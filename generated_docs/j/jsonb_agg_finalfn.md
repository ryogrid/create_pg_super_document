# jsonb_agg_finalfn

## Location
[src/backend/utils/adt/jsonb.c:1640-1672](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L1640-L1672)

## Overview
Final function for JSONB array aggregation that converts the accumulated state into the final JSONB array result.

## Definition


## Detailed Description
This function serves as the final function for JSONB array aggregation operations. It takes the accumulated JsonbAggState from the transition phase and converts it into a complete JSONB array by adding the final array end marker. The function performs a shallow clone of the parse state to ensure the final function can be called multiple times safely without modifying the original state. It handles the case where no input values were provided by returning NULL.

## Parameters / Member Variables
- : Function call information structure containing the aggregate state (JsonbAggState pointer)

## Dependencies
- Functions called/Symbols referenced:
  - [JsonbAggState](../J/JsonbAggState.md)
  - [JsonbInState](../J/JsonbInState.md)
  - Jsonb
  - [AggCheckCallContext](../A/AggCheckCallContext.md)
  - [clone_parse_state](../c/clone_parse_state.md)
  - [pushJsonbValue](../p/pushJsonbValue.md)
  - WJB_END_ARRAY
  - [JsonbValueToJsonb](../J/JsonbValueToJsonb.md)
- Called from (representative examples):
  - PostgreSQL aggregate framework (no direct code references found)

## Notes and Other Information
- Cannot be called directly due to internal-type argument requirement
- Returns NULL if no input values were provided to the aggregate
- Uses shallow cloning to allow multiple final function calls
- Only adds the final array end marker without modifying existing values
- Part of PostgreSQL's JSONB aggregate function family located in src/backend/utils/adt/jsonb.c:1640-1672