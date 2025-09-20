# int8range_subdiff

## Location
[src/backend/utils/adt/rangetypes.c:1630-1638](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L1630-L1638)

## Overview
Computes the difference between two bigint values for use in range type operations, returning the result as a float8 value.

## Definition

```c
Datum
int8range_subdiff(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is used as a subdiff function for int8range (bigint range) types in PostgreSQL. It takes two bigint (int64) values and computes their difference, converting the result to a float8 (double precision) value. This function is typically used internally by range operations that need to calculate the "size" or difference between range boundaries, such as range selectivity estimation or range operator implementations.

The function performs a simple arithmetic subtraction of the second parameter from the first, with type conversion from int64 to float8 to handle potential overflow situations and provide a consistent numeric result type across different range subdiff functions.

## Parameters / Member Variables
- : First bigint value (int64) - the minuend in the subtraction operation
- : Second bigint value (int64) - the subtrahend in the subtraction operation

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64 (macro for extracting int64 arguments)
  - PG_RETURN_FLOAT8 (macro for returning float8 values)

## Notes and Other Information
- This function is part of the range types subdiff function family, alongside similar functions for other data types (int4range_subdiff, numrange_subdiff, etc.)
- The result is returned as float8 to provide sufficient precision and range for representing the difference
- Located in src/backend/utils/adt/rangetypes.c:1630-1638
- Used internally by PostgreSQL's range type system for operations requiring difference calculations between range bounds