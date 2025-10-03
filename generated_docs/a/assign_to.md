# assign_to

## Location
[src/backend/snowball/libstemmer/utilities.c:466-477](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/utilities.c#L466-L477)

## Overview
A function in the Snowball stemming library that copies the entire working buffer to a destination buffer, essentially making a complete copy of the current string.

## Definition

```c
}

extern symbol * assign_to(struct SN_env * z, symbol * p)
```
## Detailed Description
The  function performs a complete assignment of the working buffer contents to the destination buffer. Unlike  which copies only a selected slice, this function copies the entire string from position 0 to the current length (). The function handles memory management automatically, expanding the destination buffer if necessary to accommodate the entire working string.

This operation is useful when you need to preserve the complete state of the working buffer at a particular point in the stemming process, or when copying the final result to an output buffer.

## Parameters / Member Variables
- `*z`: Pointer to the Snowball environment structure containing the working string and its length
- `*p`: Destination buffer where the entire working string will be copied (may be reallocated if too small)
## Dependencies
- Functions called/Symbols referenced:
  - CAPACITY (macro to get buffer capacity)
  - [increase_size](../i/increase_size.md) (expands buffer if needed to fit the entire string)
  - memmove (copies the entire string memory safely)
  - SET_SIZE (macro to set the destination buffer size)
  - symbol (character type used in buffers)
- Called from (representative examples):
  - [among](among.md) (utility function for pattern matching operations)

## Notes and Other Information
- Returns the destination buffer pointer on success, NULL on memory allocation failure
- Copies the entire working buffer from position 0 to position  (current length)
- Automatically handles memory reallocation if the destination buffer capacity is insufficient
- Uses memmove for safe memory copying that handles overlapping memory regions
- The destination buffer follows Snowball's variable-length string format with embedded size and capacity
- Part of the external API for Snowball stemmer implementations
- More straightforward than slice_to as it doesn't need boundary checking - always copies the full valid string
- Commonly used for preserving intermediate results or copying final stemming outputs