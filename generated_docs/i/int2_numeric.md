# int2_numeric

## Location
[src/backend/utils/adt/numeric.c:4560-4568](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L4560-L4568)

## Overview
Converts a 16-bit signed integer (int2/smallint) to PostgreSQL's numeric data type.

## Definition
```c
Datum int2_numeric(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL SQL-callable function that converts an int2 (16-bit signed integer, also known as smallint in SQL) to the numeric data type. It follows PostgreSQL's function calling convention by taking PG_FUNCTION_ARGS as its parameter and returning a Datum. The function extracts the int16 value from the function arguments, converts it to int64 (which is always safe since int16 fits entirely within int64 range), then uses the internal int64_to_numeric conversion function to create the final numeric result.

## Parameters / Member Variables
- Extracts: 16-bit signed integer value from argument 0

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT16 (macro to extract int16 from function arguments)
  - [int64_to_numeric](int64_to_numeric.md) (internal conversion function from int64 to numeric)
  - PG_RETURN_NUMERIC (macro to return numeric as Datum)
- Called from (representative examples):
  - [JsonItemFromDatum](../J/JsonItemFromDatum.md) (in JSON processing for type conversions)

## Notes and Other Information
- This is a SQL-callable function that can be invoked from PostgreSQL SQL statements
- Part of PostgreSQL's type conversion system between integer and numeric types
- The conversion from int16 to numeric is always safe since numeric can represent any 16-bit integer value exactly
- Used internally by PostgreSQL's JSON processing engine for type conversions
- Commonly used in SQL casts like `CAST(smallint_value AS numeric)` or `smallint_value::numeric`
- The function implicitly promotes int16 to int64 before calling the conversion routine, which is a safe widening conversion