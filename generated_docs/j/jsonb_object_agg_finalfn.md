# jsonb_object_agg_finalfn

## Location
[src/backend/utils/adt/jsonb.c:1930-1967](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L1930-L1967)

## Overview
Final function for JSONB object aggregation that converts the accumulated aggregate state into the final JSONB object result.

## Definition
```c
Datum jsonb_object_agg_finalfn(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the final function for all JSONB object aggregation operations in PostgreSQL. It takes the accumulated state from the transition functions and produces the final JSONB object. The function performs a shallow clone of the aggregate state's parse state to ensure that multiple calls to the final function don't modify the original aggregate state. It then closes the JSONB object construction by adding a WJB_END_OBJECT marker and converts the resulting JsonbValue structure into a complete Jsonb object.

The function includes safety checks to ensure it's called within a proper aggregate context and handles null input by returning null, which occurs when no input values were provided to the aggregate.

## Parameters / Member Variables
- Input parameter:
  - arg: JsonbAggState pointer containing the accumulated aggregate state
- Local variables:
  - result: JsonbInState structure for building the final result
  - out: Final Jsonb object to be returned

## Dependencies
- Functions called/Symbols referenced:
  - [AggCheckCallContext](../A/AggCheckCallContext.md)
  - [clone_parse_state](../c/clone_parse_state.md)
  - [pushJsonbValue](../p/pushJsonbValue.md)
  - [JsonbValueToJsonb](../J/JsonbValueToJsonb.md)
- Types referenced:
  - [JsonbAggState](../J/JsonbAggState.md)
  - [JsonbInState](../J/JsonbInState.md)
  - Jsonb
  - WJB_END_OBJECT
- Called from (representative examples):
  - PostgreSQL aggregate execution engine during final phase of jsonb_object_agg operations

## Notes and Other Information
- This function is shared by all variants of jsonb_object_agg (regular, unique, strict, etc.)
- Performs a shallow clone of the parse state to protect against multiple final function calls
- Returns NULL if no input values were provided to the aggregate
- Contains assertion to ensure proper aggregate calling context
- Located in src/backend/utils/adt/jsonb.c:1930-1967
- The shallow clone strategy is sufficient since the function only adds the end marker without modifying existing values