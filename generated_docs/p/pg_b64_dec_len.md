# pg_b64_dec_len

## Location
[src/common/base64.c:239-242](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/base64.c#L239-L242)

## Overview
Calculates the maximum buffer size needed for decoding a base64-encoded string of given length.

## Definition
```c
int pg_b64_dec_len(int srclen)
```

## Detailed Description
`pg_b64_dec_len` is a utility function that estimates the maximum number of bytes needed to store the decoded binary data from a base64-encoded string. This is essential for proper buffer allocation before calling `pg_b64_decode`.

The calculation uses the formula `(srclen * 3) >> 2`, which is equivalent to `(srclen * 3) / 4`. This is based on the base64 encoding principle where every 4 characters of base64 input can produce up to 3 bytes of binary output. The bit shift operation (>> 2) is used for efficient division by 4.

Note that this function returns the maximum possible decoded length, not the exact length, since base64 padding characters ('=') at the end of the input may result in fewer actual decoded bytes.

## Parameters / Member Variables
- `srclen`: Length of the base64-encoded string in characters for which to calculate the maximum decoded length

## Dependencies
- Functions called/Symbols referenced: None (pure calculation)

- Called from (representative examples):
  - `[scram_verify_plain_password](../s/scram_verify_plain_password.md)` (src/backend/libpq/auth-scram.c:540)
  - `[parse_scram_secret](parse_scram_secret.md)` (src/backend/libpq/auth-scram.c:637, 648, 656)
  - `[read_client_final_message](../r/read_client_final_message.md)` (src/backend/libpq/auth-scram.c:1373)
  - `[read_server_first_message](../r/read_server_first_message.md)` (src/interfaces/libpq/fe-auth-scram.c:648)

## Notes and Other Information
- Returns the maximum number of bytes that could result from decoding
- The actual decoded length may be 0-2 bytes less due to base64 padding
- Used for buffer allocation before calling `pg_b64_decode`
- Efficient bit shift operation for division by 4
- Commonly used in SCRAM authentication for sizing buffers for decoded cryptographic data
- Simple mathematical formula with no error conditions
- Essential for preventing buffer overflows in decoding operations
- Provides a safe upper bound for buffer allocation