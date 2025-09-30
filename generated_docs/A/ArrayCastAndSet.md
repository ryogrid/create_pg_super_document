# ArrayCastAndSet

## Location
[src/backend/utils/adt/arrayfuncs.c:4815-4853](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L4815-L4853)

## Overview
A static utility function that copies a PostgreSQL Datum value to a destination buffer with proper alignment and returns the total space consumed.

## Definition
```c
static int ArrayCastAndSet(Datum src, int typlen, bool typbyval, char typalign, char *dest)
```

## Detailed Description
This function handles the complex task of copying PostgreSQL Datum values into array storage with proper memory alignment and space calculation. It supports both fixed-length and variable-length data types, handling by-value and by-reference storage modes appropriately.

For fixed-length types (typlen > 0), it uses either `store_att_byval` for by-value types or `memmove` for by-reference types. For variable-length types (typlen <= 0), it calculates the actual length using `att_addlength_datum` and copies the data with `memmove`. In both cases, it ensures proper alignment using `att_align_nominal` and returns the total space consumed including any padding.

The function assumes the caller has already handled NULL values and provides the foundation for array element storage operations.

## Parameters
- `src`: Source Datum value to be copied
- `typlen`: Type length specification (-1 for variable-length, -2 for null-terminated strings, positive for fixed-length)
- `typbyval`: Boolean indicating whether the type is stored by value (true) or by reference (false)
- `typalign`: Character indicating alignment requirement ('c\ = char, 's\ = short, 'i\ = int, 'd\ = double)
- `dest`: Destination buffer where the value should be stored

## Dependencies
- Functions called/Symbols referenced:
  - [store_att_byval](../s/store_att_byval.md) (stores by-value attributes)
  - att_align_nominal (calculates aligned size)
  - att_addlength_datum (calculates actual length of variable-length datum)
  - [DatumGetPointer](../D/DatumGetPointer.md) (converts Datum to pointer for by-reference types)
- Called from (representative examples):
  - [CopyArrayEls](../C/CopyArrayEls.md)
  - [array_set_element](../a/array_set_element.md)
  - [array_fill_internal](../a/array_fill_internal.md)

## Notes and Other Information
- Returns the total number of bytes used including alignment padding
- The caller is responsible for null checking before calling this function
- For variable-length types, asserts that typbyval is false (variable-length types cannot be by-value)
- Uses efficient memory operations (memmove) for copying data
- Part of PostgreSQL's internal array support routines
- The function is static, meaning it's only accessible within the arrayfuncs.c compilation unit
- Critical for maintaining proper memory layout in PostgreSQL arrays
- Handles the complexity of PostgreSQL's diverse type system in a unified interface

## Simplified Source

```c
static int ArrayCastAndSet(Datum src, int typlen, bool typbyval, char typalign, char *dest) {
    int inc;

    if (typlen > 0) {
        // Fixed-length type
        if (typbyval) {
            // Store by-value type directly
            store_att_byval(dest, src, typlen);
        } else {
            // Copy by-reference type data
            memmove(dest, DatumGetPointer(src), typlen);
        }
        // Calculate aligned size for fixed-length type
        inc = att_align_nominal(typlen, typalign);
    } else {
        // Variable-length type (must be by-reference)
        Assert(!typbyval);

        // Calculate actual length of variable-length data
        inc = att_addlength_datum(0, typlen, src);

        // Copy the variable-length data
        memmove(dest, DatumGetPointer(src), inc);

        // Apply alignment padding
        inc = att_align_nominal(inc, typalign);
    }

    return inc;  // Total bytes used including padding
}
```