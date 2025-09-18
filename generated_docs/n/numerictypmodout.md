# numerictypmodout

## Location
src/backend/utils/adt/numeric.c: 1367 - 1390

## Overview
The  function converts internal NUMERIC type modifier values back into human-readable string format for display purposes.

## Definition


## Detailed Description
This function is the output counterpart to , serving to convert internal typmod representations back into displayable strings. When PostgreSQL needs to show a NUMERIC type with its precision and scale constraints (such as in table definitions or error messages), this function formats the typmod into a string like "(10,2)" representing precision and scale. If the typmod is invalid, it returns an empty string.

## Parameters / Member Variables
- : Internal type modifier value containing encoded precision and scale information (PG_GETARG_INT32(0))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32: Extracts int32 argument from function call
  - [palloc](../p/palloc.md): PostgreSQL memory allocation function
  - [is_valid_numeric_typmod](../i/is_valid_numeric_typmod.md): Validates the typmod value
  - snprintf: Standard C string formatting function
  - [numeric_typmod_precision](numeric_typmod_precision.md): Extracts precision from typmod
  - [numeric_typmod_scale](numeric_typmod_scale.md): Extracts scale from typmod
  - PG_RETURN_CSTRING: Returns C string result

- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- Allocates 64 bytes for the output string, sufficient for any valid precision/scale combination
- Returns formatted string in the form "(precision,scale)"
- Returns empty string for invalid typmod values
- Part of PostgreSQL's type system output machinery
- Commonly used when displaying table schemas or in error messages involving NUMERIC types
- Located in src/backend/utils/adt/numeric.c:1367-1390