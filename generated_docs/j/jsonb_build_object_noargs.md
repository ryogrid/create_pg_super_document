# jsonb_build_object_noargs

## Location
[src/backend/utils/adt/jsonb.c:1197-1209](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L1197-L1209)

## Overview
A PostgreSQL function that handles the degenerate case of  when called with zero arguments, returning an empty JSONB object.

## Definition


## Detailed Description
This function is specifically designed to handle the case where  is called without any arguments. It creates an empty JSONB object  by directly constructing the JSONB structure without processing any key-value pairs. This is a performance optimization that avoids the overhead of variadic argument processing when no arguments are provided.

## Parameters / Member Variables
- : Function call information structure (unused in this case since no arguments are expected)

## Dependencies
- Functions called/Symbols referenced:
  - [JsonbInState](../J/JsonbInState.md) (struct)
  - [pushJsonbValue](../p/pushJsonbValue.md)
  - [JsonbValueToJsonb](../J/JsonbValueToJsonb.md)
  - WJB_BEGIN_OBJECT
  - WJB_END_OBJECT
  - PG_RETURN_POINTER
- Called from (representative examples):
  - No direct callers found (SQL function entry point)

## Notes and Other Information
- This function is used as an optimization for the zero-argument case of 
- It directly constructs an empty JSONB object without going through the variadic argument extraction process
- The resulting JSONB object is always 
- This is a PostgreSQL internal function that may be called when the SQL parser determines that  has zero arguments