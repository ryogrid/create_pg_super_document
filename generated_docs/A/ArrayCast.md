# ArrayCast

## Location
src/backend/utils/adt/arrayfuncs.c: 4804 - 4814

## Overview
A static utility function that fetches an array element value from memory and converts it correctly to a PostgreSQL Datum type.

## Definition
```c
static Datum ArrayCast(char *value, bool byval, int len)
```

## Detailed Description
This function serves as a wrapper around the lower-level `fetch_att` function to properly convert an array element stored in memory into a PostgreSQL Datum. The function handles the complexity of PostgreSQL's variable-length data representation, where different data types may be stored by-value or by-reference with different alignment and length requirements.

The function assumes that the caller has already determined that the element is not NULL - null handling must be performed separately before calling this function. It delegates the actual conversion work to `fetch_att`, which handles the low-level details of extracting values based on the type's storage characteristics.

## Parameters
- `value`: Pointer to the raw array element data in memory
- `byval`: Boolean indicating whether the data type is stored by value (true) or by reference (false)
- `len`: Length specification for the data type (-1 for variable-length, positive number for fixed-length)

## Dependencies
- Functions called/Symbols referenced:
  - fetch_att (performs the actual attribute fetching and conversion)
- Called from (representative examples):
  - array_get_element

## Notes and Other Information
- The caller is responsible for null checking before calling this function
- This is a thin wrapper that provides semantic clarity for array-specific operations
- Part of PostgreSQL's internal array support routines  
- The function is static, meaning it's only accessible within the arrayfuncs.c compilation unit
- The byval and len parameters must match the characteristics of the array's element type
- Returns a properly formed Datum that can be used in PostgreSQL's expression evaluation system