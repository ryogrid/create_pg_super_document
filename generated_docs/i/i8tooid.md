# i8tooid

## Location
src/backend/utils/adt/int8.c: 1353 - 1365

## Overview
Converts a PostgreSQL int8 (64-bit integer) value to an OID (Object Identifier) with strict range validation.

## Definition
```c
Datum i8tooid(PG_FUNCTION_ARGS)
```

## Detailed Description
This function performs a type conversion from PostgreSQL's 64-bit integer type (int8) to OID (Object Identifier). OIDs in PostgreSQL are unsigned 32-bit integers, so this function must validate that the input int64 value falls within the valid OID range (0 to PG_UINT32_MAX). The function performs explicit range checking and throws an error if the value is negative or exceeds the maximum OID value.

## Parameters / Member Variables
- The function uses PostgreSQL's `PG_FUNCTION_ARGS` macro to access arguments
- Argument 0: An int8 (64-bit integer) value to be converted to OID

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64 (macro to extract int64 argument)
  - PG_UINT32_MAX (PostgreSQL constant defining maximum unsigned 32-bit value)
  - ereport (PostgreSQL error reporting function)
  - PG_RETURN_OID (macro to return OID result)
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- Located in src/backend/utils/adt/int8.c:1353-1365
- This is a PostgreSQL built-in function that can be invoked from SQL
- Enforces strict range checking: values must be >= 0 and <= PG_UINT32_MAX (4294967295)
- Throws ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE error with "OID out of range" message for invalid inputs
- OIDs are fundamental to PostgreSQL's internal object management system
- Part of PostgreSQL's type system for safe conversions to OID type
- Uses unlikely() hints for branch prediction optimization on error conditions