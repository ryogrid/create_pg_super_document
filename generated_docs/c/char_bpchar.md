# char_bpchar

## Location
[src/backend/utils/adt/varchar.c:353-370](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varchar.c#L353-L370)

## Overview
Converts a single character (char) to a bpchar(1) value, creating a blank-padded character type with length 1.

## Definition
```c
Datum char_bpchar(PG_FUNCTION_ARGS)
```

## Detailed Description
The `char_bpchar` function is a PostgreSQL type conversion function that converts a single character value (the `char` type) to a bpchar with length 1. This function creates a variable-length bpchar structure containing exactly one character.

The function allocates a new bpchar structure with the appropriate variable-length header and copies the single character into the data portion. Since the target is exactly one character, no padding is required, and the conversion is straightforward.

## Parameters / Member Variables
- `c` (char): Input character value to be converted to bpchar(1)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CHAR
  - [palloc](../p/palloc.md)
  - SET_VARSIZE
  - VARDATA
  - PG_RETURN_BPCHAR_P
- Called from (representative examples):
  - None found in current analysis

## Notes and Other Information
- This function enables seamless conversion between PostgreSQL's char and bpchar(1) types
- The result always has a length of exactly 1 character (plus variable-length header)
- Memory is allocated using palloc for proper PostgreSQL memory context management
- Part of PostgreSQL's type conversion system that supports implicit and explicit casts
- The resulting bpchar(1) can be further processed by other bpchar functions if needed

## Simplified Source

```c
Datum char_bpchar(PG_FUNCTION_ARGS) {
    // Get the input character
    char c = PG_GETARG_CHAR(0);

    // Allocate space for bpchar(1): header + 1 character
    BpChar *result = (BpChar *) palloc(VARHDRSZ + 1);

    // Set the size and store the character
    SET_VARSIZE(result, VARHDRSZ + 1);
    *(VARDATA(result)) = c;

    PG_RETURN_BPCHAR_P(result);
}
```