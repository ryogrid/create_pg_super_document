# pg_b64_enc_len

## Location
[src/common/base64.c:224-238](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/base64.c#L224-L238)

## Overview
Calculates the required buffer size for base64 encoding a given amount of binary data.

## Definition
```c
int pg_b64_enc_len(int srclen)
```

## Detailed Description
`pg_b64_enc_len` is a utility function that calculates the exact number of characters needed to store the base64-encoded representation of binary data. This is essential for proper buffer allocation before calling `pg_b64_encode`.

The calculation is based on the base64 encoding principle where every 3 bytes of input data produces exactly 4 characters of base64 output. The function uses the formula `(srclen + 2) / 3 * 4` which:
1. Adds 2 to the source length to handle partial groups (rounds up division)
2. Divides by 3 to get the number of 3-byte input groups
3. Multiplies by 4 to get the number of output characters

This ensures that even partial final groups (1 or 2 bytes) are properly accounted for with the necessary padding characters.

## Parameters / Member Variables
- `srclen`: Length of the source binary data in bytes for which to calculate the encoded length

## Dependencies
- Functions called/Symbols referenced: None (pure calculation)

- Called from (representative examples):
  - `mock_scram_secret` (src/backend/libpq/auth-scram.c:707)
  - `build_server_first_message` (src/backend/libpq/auth-scram.c:1229)
  - `scram_build_secret` (src/common/scram-common.c:254, 255, 256)
  - `build_client_first_message` (src/interfaces/libpq/fe-auth-scram.c:364)

## Notes and Other Information
- Returns the exact number of characters needed for base64 encoding
- Does not include space for null termination (caller must add 1 if needed)
- Used for buffer allocation before calling `pg_b64_encode`
- The calculation handles padding correctly for any input length
- Commonly used in SCRAM authentication for sizing buffers for encoded cryptographic data
- Simple mathematical formula with no error conditions
- Essential for preventing buffer overflows in encoding operations