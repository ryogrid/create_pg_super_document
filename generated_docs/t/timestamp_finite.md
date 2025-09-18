# timestamp_finite

## Location
[src/backend/utils/adt/timestamp.c:2147-2154](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L2147-L2154)

## Overview
A PostgreSQL SQL function that checks whether a timestamp value is finite (not infinity or -infinity).

## Definition
```c
Datum timestamp_finite(PG_FUNCTION_ARGS)
```

## Detailed Description
The timestamp_finite function is a public PostgreSQL function that determines if a given timestamp is finite. It extracts a timestamp argument from the function call using PG_GETARG_TIMESTAMP and checks if it represents a finite value by using the TIMESTAMP_NOT_FINITE macro. The function returns true if the timestamp is finite (a real date/time), and false if it represents positive or negative infinity.

## Parameters / Member Variables
- Function argument 0: A timestamp value to be checked for finiteness

## Dependencies
- Functions called/Symbols referenced:
  - Timestamp (timestamp data type)
  - PG_GETARG_TIMESTAMP (macro to extract timestamp argument)
  - TIMESTAMP_NOT_FINITE (macro to check if timestamp is infinite)
  - PG_RETURN_BOOL (macro to return boolean result)
- Called from (representative examples):
  - No direct references found (likely called via SQL function calls)

## Notes and Other Information
This is a public PostgreSQL function that can be called from SQL queries using the isfinite() function. PostgreSQL timestamps support special values including positive infinity ('infinity') and negative infinity ('-infinity') to represent unbounded time ranges. This function provides a way to distinguish between finite timestamp values and these special infinite values. The function follows PostgreSQL's standard function calling conventions using the PG_FUNCTION_ARGS framework.