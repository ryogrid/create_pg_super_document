# jsonb_build_array_noargs

## Location
[src/backend/utils/adt/jsonb.c:1258-1278](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L1258-L1278)

## Overview
A PostgreSQL function that handles the degenerate case of  when called with zero arguments, returning an empty JSONB array.

## Definition

```c
Datum
jsonb_build_array_noargs(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is specifically designed to handle the case where  is called without any arguments. It creates an empty JSONB array  by directly constructing the JSONB structure without processing any elements. This is a performance optimization that avoids the overhead of variadic argument processing when no arguments are provided.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Function call information structure (unused in this case since no arguments are expected)
## Dependencies
- Functions called/Symbols referenced:
  - [JsonbInState](../J/JsonbInState.md) (struct)
  - [pushJsonbValue](../p/pushJsonbValue.md)
  - [JsonbValueToJsonb](../J/JsonbValueToJsonb.md)
  - WJB_BEGIN_ARRAY
  - WJB_END_ARRAY
  - PG_RETURN_POINTER
- Called from (representative examples):
  - No direct callers found (SQL function entry point)

## Notes and Other Information
- This function is used as an optimization for the zero-argument case of 
- It directly constructs an empty JSONB array without going through the variadic argument extraction process
- The resulting JSONB array is always 
- This is a PostgreSQL internal function that may be called when the SQL parser determines that  has zero arguments
- Mirrors the functionality of  but for arrays instead of objects

## Simplified Source

```c
Datum
jsonb_build_array_noargs(PG_FUNCTION_ARGS)
{
    // Initialize empty JSONB state
    JsonbInState result;
    memset(&result, 0, sizeof(JsonbInState));

    // Create empty array: start with BEGIN_ARRAY, then immediately END_ARRAY
    pushJsonbValue(&result.parseState, WJB_BEGIN_ARRAY, NULL);
    result.res = pushJsonbValue(&result.parseState, WJB_END_ARRAY, NULL);

    // Convert to JSONB and return
    PG_RETURN_POINTER(JsonbValueToJsonb(result.res));
}
```