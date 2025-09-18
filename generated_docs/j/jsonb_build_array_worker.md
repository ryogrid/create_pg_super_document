# jsonb_build_array_worker

## Location
[src/backend/utils/adt/jsonb.c:1210-1236](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L1210-L1236)

## Overview
A core PostgreSQL function that constructs a JSONB array from an array of arguments, with support for optional null value omission.

## Definition


## Detailed Description
This function is the workhorse for JSONB array construction in PostgreSQL. It takes an array of arguments along with their nullness indicators and types, and constructs a JSONB array. The function supports an  flag that allows null values to be omitted from the resulting array rather than being included as JSON null values. This function is used by both the SQL  function and internal PostgreSQL execution routines for JSON constructor expressions.

## Parameters / Member Variables
- : Number of arguments to process into the array
- : Array of Datum values representing the array elements
- : Array of boolean flags indicating which arguments are NULL
- : Array of OID values representing the PostgreSQL types of each argument
- : Boolean flag that when true causes NULL values to be omitted from the array

## Dependencies
- Functions called/Symbols referenced:
  - [JsonbInState](../J/JsonbInState.md) (struct)
  - [pushJsonbValue](../p/pushJsonbValue.md)
  - [add_jsonb](../a/add_jsonb.md)
  - [JsonbValueToJsonb](../J/JsonbValueToJsonb.md)
  - [JsonbPGetDatum](../J/JsonbPGetDatum.md)
  - WJB_BEGIN_ARRAY
  - WJB_END_ARRAY
- Called from (representative examples):
  - [jsonb_build_array](jsonb_build_array.md)
  - [ExecEvalJsonConstructor](../E/ExecEvalJsonConstructor.md)
  - PG_RETURN_JSONB_P

## Notes and Other Information
- This function is used internally by both SQL functions and expression evaluation
- The  parameter provides flexibility for different JSON construction semantics
- When  is true, null values are completely skipped rather than being added as JSON null
- The function handles type conversion through the  helper function
- Used in JSON constructor expressions in SQL queries for performance optimization