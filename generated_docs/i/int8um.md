# int8um

## Location
[src/backend/utils/adt/int8.c:440-453](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L440-L453)

## Overview
The int8um function implements unary minus (negation) operation for 64-bit signed integers (bigint) in PostgreSQL, handling overflow conditions and returning the negated value.

## Definition

```c
Datum
int8um(PG_FUNCTION_ARGS)
```
## Detailed Description
This function performs unary minus operation on a 64-bit signed integer argument. It extracts the input argument using PostgreSQL's function argument macros, checks for potential overflow conditions (specifically when the input is the minimum possible 64-bit integer value), and returns the negated result. The function is part of PostgreSQL's arithmetic operators for the bigint data type and follows the standard PostgreSQL function calling convention using the Datum return type and PG_FUNCTION_ARGS parameter mechanism.

## Parameters / Member Variables
- The function uses PostgreSQL's standard function argument mechanism where arguments are accessed via PG_GETARG_INT64(0) macro to retrieve the first (and only) int64 argument

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64 (macro for extracting int64 argument)
  - PG_INT64_MIN (minimum value constant for int64)
  - PG_RETURN_INT64 (macro for returning int64 result)
  - ereport (error reporting function)
- Called from: 
  - This function is typically invoked through PostgreSQL's function call mechanism for bigint unary minus operations

## Notes and Other Information
- The function specifically checks for overflow when the input is PG_INT64_MIN (the most negative 64-bit integer), as negating this value would exceed the positive range of 64-bit signed integers
- Error handling follows PostgreSQL conventions by using ereport with ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE
- Located in src/backend/utils/adt/int8.c:440-453, part of the arithmetic operators section for 64-bit integers