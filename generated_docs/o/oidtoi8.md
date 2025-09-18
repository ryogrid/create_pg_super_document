# oidtoi8

## Location
[src/backend/utils/adt/int8.c:1366-1376](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L1366-L1376)

## Overview
Converts an Oid (object identifier) value to a 64-bit signed integer (int8/bigint).

## Definition


## Detailed Description
This function is a PostgreSQL built-in function that performs a type conversion from the Oid data type to int8 (bigint). It takes an Oid value as input and returns it as a 64-bit signed integer. The conversion is straightforward since both types are numeric, with Oid being an unsigned 32-bit integer that fits comfortably within the range of a 64-bit signed integer.

The function is implemented as a PostgreSQL internal function using the standard function calling convention (PG_FUNCTION_ARGS macro) and follows the typical pattern for type conversion functions in PostgreSQL.

## Parameters / Member Variables
- Input (via PG_GETARG_OID(0)): An Oid value to be converted to int8
- Return: A Datum containing the int8 representation of the input Oid

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID (macro for extracting Oid argument)
  - PG_RETURN_INT64 (macro for returning int64 value)

- Called from (representative examples):
  - No direct references found in the codebase (likely called through SQL type conversion)

## Notes and Other Information
- This function is typically invoked through PostgreSQL's type conversion system when casting from oid to bigint
- The conversion is always safe since Oid values (32-bit unsigned) fit within the int8 range (64-bit signed)
- Located in src/backend/utils/adt/int8.c:1366-1376
- Part of PostgreSQL's comprehensive set of type conversion functions