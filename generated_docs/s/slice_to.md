# slice_to

## Location
[src/backend/snowball/libstemmer/utilities.c:448-465](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/utilities.c#L448-L465)

## Overview
A function in the Snowball stemming library that copies the current slice (between bra and ket positions) from the working buffer to a destination buffer.

## Definition

```c
}

extern symbol * slice_to(struct SN_env * z, symbol * p)
```
## Detailed Description
The  function extracts the currently selected slice (the substring between  and  positions) from the working buffer and copies it to the provided destination buffer. The function handles memory management by expanding the destination buffer if necessary and properly setting its size. If the slice boundaries are invalid, it cleans up the destination buffer and returns NULL.

This function is essential for extracting parts of words during stemming operations, allowing stemmers to save portions of the original word for later use or analysis.

## Parameters / Member Variables
- : Pointer to the Snowball environment structure containing the working string and cursor positions
- : Destination buffer where the slice will be copied (may be reallocated if too small)

## Dependencies
- Functions called/Symbols referenced:
  - [slice_check](slice_check.md) (validates slice boundaries before operation)
  - [lose_s](../l/lose_s.md) (cleans up buffer memory on error)
  - CAPACITY (macro to get buffer capacity)
  - [increase_size](../i/increase_size.md) (expands buffer if needed)
  - memmove (copies memory safely)
  - SET_SIZE (macro to set buffer size)
  - symbol (character type used in buffers)
- Called from (representative examples):
  - [r_undouble](../r/r_undouble.md) (in Danish stemmer)
  - [r_tidy](../r/r_tidy.md) (in Finnish stemmer) 
  - [among](../a/among.md) (utility function for pattern matching)

## Notes and Other Information
- Returns the destination buffer pointer on success, NULL on error
- Automatically handles memory reallocation if the destination buffer is too small
- Performs boundary checking via slice_check to ensure valid slice operations
- The slice length is calculated as 
- Uses memmove for safe memory copying that handles overlapping regions
- Part of the external API for Snowball stemmer implementations
- The destination buffer uses the Snowball variable-length string format with embedded size and capacity
- On error, cleans up the destination buffer to prevent memory leaks