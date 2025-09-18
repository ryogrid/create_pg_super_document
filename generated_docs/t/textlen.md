# textlen

## Location
[src/backend/utils/adt/varlena.c:693-710](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L693-L710)

## Overview
Returns the logical character length of a text value, optimizing for cases where the text may be compressed.

## Definition
```c
Datum textlen(PG_FUNCTION_ARGS)
```

## Detailed Description
The `textlen` function calculates and returns the logical length (number of characters) of a PostgreSQL text value. Unlike simply using VARSIZE, this function returns the actual character count, which is particularly important for variable-length text data. The function is designed to avoid decompressing the argument when possible, making it more efficient for compressed text values.

This function serves as a SQL-callable wrapper around the internal `text_length` function, providing the logical character count rather than the physical storage size.

## Parameters / Member Variables
- Input: A text datum (accessed via PG_GETARG_DATUM(0))
- Return: An int32 representing the character length (returned via PG_RETURN_INT32)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_DATUM (macro for extracting datum argument)
  - [text_length](text_length.md) (internal function to calculate text length)
  - PG_RETURN_INT32 (macro for returning 32-bit integer result)

- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- The function is located in src/backend/utils/adt/varlena.c at lines 693-710
- Part of the PUBLIC ROUTINES section, indicating it's intended for external/SQL use
- Optimized to avoid decompression when the text argument is compressed
- Returns logical character count, not physical storage bytes
- The logical length can be less than VARSIZE due to storage overhead and compression
- Commonly used in SQL LENGTH() function implementations