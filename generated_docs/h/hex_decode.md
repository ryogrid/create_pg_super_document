# hex_decode

## Location
[src/backend/utils/adt/encode.c:190-195](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/encode.c#L190-L195)

## Overview
Utility function that converts hexadecimal string representation back to binary data, serving as a wrapper around the safer  function.

## Definition

```c
uint64
hex_decode(const char *src, size_t len, char *dst)
```
## Detailed Description
The `hex_decode` function provides a simple interface for hexadecimal decoding operations. It acts as a wrapper around `hex_decode_safe`, providing the same functionality but without the enhanced error context handling. The function converts a hexadecimal string (e.g., "48656c6c6f") back into its binary representation (e.g., "Hello").

This is the standard entry point for hex decoding when error context handling is not required, making it suitable for situations where simple success/failure indication is sufficient.

## Parameters / Member Variables
- `src`: Pointer to source hexadecimal string to be decoded
- `len`: Length of the source hexadecimal string in characters
- `dst`: Pointer to destination buffer for the decoded binary data (must be pre-allocated with at least len/2 bytes)

## Dependencies
- Functions called/Symbols referenced:
  - `hex_decode_safe` - The underlying safe decoding function that performs the actual work
- Called from (representative examples):
  - `esc_dec_len` - Escape encoding/decoding length calculations
  - `ecpg_get_data` - ECPG data retrieval operations

## Notes and Other Information
- Simple wrapper around `hex_decode_safe` with NULL error context
- Inherits all the functionality of `hex_decode_safe`: whitespace skipping, validation, etc.
- Returns the number of bytes written to the destination buffer (always len/2 for valid input)
- Input validation ensures even number of hex digits and valid hex characters
- Skips whitespace characters (space, newline, tab, carriage return) in input
- Part of PostgreSQL's core hex decoding infrastructure
- Suitable for cases where detailed error context is not needed
- The underlying `hex_decode_safe` handles the actual character-by-character conversion using `get_hex`