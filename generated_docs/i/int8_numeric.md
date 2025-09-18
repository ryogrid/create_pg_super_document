# int8_numeric

## Location
[src/backend/utils/adt/numeric.c:4493-4500](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L4493-L4500)

## Overview
Converts a 64-bit signed integer (int8/bigint) to PostgreSQL's numeric data type.

## Definition
```c
Datum int8_numeric(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL SQL-callable function that converts an int8 (64-bit signed integer, also known as bigint in SQL) to the numeric data type. It follows PostgreSQL's function calling convention by taking PG_FUNCTION_ARGS as its parameter and returning a Datum. The function extracts the int64 value from the function arguments, converts it to numeric format using the internal conversion function, and returns the result as a numeric Datum.

## Parameters / Member Variables
- Function uses PostgreSQL's standard function argument mechanism (PG_FUNCTION_ARGS)
- Extracts: 64-bit signed integer value from argument 0

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64 (macro to extract int64 from function arguments)
  - [int64_to_numeric](int64_to_numeric.md) (internal conversion function)
  - PG_RETURN_NUMERIC (macro to return numeric as Datum)
- Called from (representative examples):
  - [executeItemOptUnwrapTarget](../e/executeItemOptUnwrapTarget.md) (in jsonpath execution)
  - [JsonItemFromDatum](../J/JsonItemFromDatum.md) (in JSON processing)

## Notes and Other Information
- This is a SQL-callable function that can be invoked from PostgreSQL SQL statements
- Part of PostgreSQL's type conversion system between integer and numeric types
- The conversion from int64 to numeric is always safe since numeric can represent any 64-bit integer value exactly
- Used internally by PostgreSQL's JSON path execution engine for type conversions