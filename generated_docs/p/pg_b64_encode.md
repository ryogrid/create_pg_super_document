# pg_b64_encode

## Location
[src/common/base64.c:49-115](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/base64.c#L49-L115)

## Overview
Encodes binary data into base64 format using the standard base64 character set without whitespace support.

## Definition
```c
int pg_b64_encode(const char *src, int len, char *dst, int dstlen)
```

## Detailed Description
`pg_b64_encode` converts binary data into a base64-encoded string representation. The function processes input data in 3-byte chunks, converting each chunk into 4 base64 characters using the standard base64 alphabet (A-Z, a-z, 0-9, +, /). When the input length is not divisible by 3, the function adds appropriate padding with '=' characters.

The encoding algorithm works by:
1. Reading input data in groups of 3 bytes (24 bits)
2. Splitting the 24 bits into four 6-bit values
3. Mapping each 6-bit value to its corresponding base64 character
4. Adding '=' padding for incomplete final groups

The function includes buffer overflow protection and will return an error if the destination buffer is too small.

## Parameters / Member Variables
- `src`: Pointer to the source binary data to be encoded
- `len`: Length of the source data in bytes
- `dst`: Destination buffer for the encoded base64 string
- `dstlen`: Size of the destination buffer in bytes

## Dependencies
- Functions called/Symbols referenced:
  - `_base64` (static lookup table for base64 characters)
  - `memset` (for error handling - zeroing destination buffer)
  - `Assert` (for debugging assertions)

- Called from (representative examples):
  - `mock_scram_secret` (src/backend/libpq/auth-scram.c:710)
  - `build_server_first_message` (src/backend/libpq/auth-scram.c:1232)
  - `scram_build_secret` (src/common/scram-common.c:278, 293, 310)
  - `build_client_first_message` (src/interfaces/libpq/fe-auth-scram.c:372)

## Notes and Other Information
- Returns the length of the encoded string on success, or -1 on error
- On error, the destination buffer is zeroed for security
- The function does not null-terminate the output string
- Primarily used in SCRAM authentication for encoding cryptographic data
- Buffer overflow protection ensures safe operation with pre-allocated buffers
- No whitespace characters are included in the output (strict base64 format)