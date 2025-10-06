# numerictypmodout

## Location
[src/backend/utils/adt/numeric.c:1367-1390](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L1367-L1390)

## Overview
The  function converts internal NUMERIC type modifier values back into human-readable string format for display purposes.

## Definition

```c
Datum
numerictypmodout(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is the output counterpart to , serving to convert internal typmod representations back into displayable strings. When PostgreSQL needs to show a NUMERIC type with its precision and scale constraints (such as in table definitions or error messages), this function formats the typmod into a string like "(10,2)" representing precision and scale. If the typmod is invalid, it returns an empty string.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Internal type modifier value containing encoded precision and scale information (PG_GETARG_INT32(0))
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

## Simplified Source

```c
Datum numerictypmodout(PG_FUNCTION_ARGS) {
    int32 typmod = PG_GETARG_INT32(0);
    char *res = (char *) palloc(64);

    // Format valid typmod as "(precision,scale)"
    if (is_valid_numeric_typmod(typmod))
        snprintf(res, 64, "(%d,%d)",
                numeric_typmod_precision(typmod),
                numeric_typmod_scale(typmod));
    else
        *res = '\0';  // Empty string for invalid typmod

    PG_RETURN_CSTRING(res);
}
```