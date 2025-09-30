# pg_b64_decode

## Location
[src/common/base64.c:116-223](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/base64.c#L116-L223)

## Overview
Decodes base64-encoded strings back into binary data, performing strict validation without whitespace support.

## Definition
```c
int pg_b64_decode(const char *src, int len, char *dst, int dstlen)
```

## Detailed Description
`pg_b64_decode` converts base64-encoded strings back to their original binary representation. The function strictly validates the input format, rejecting any whitespace characters and ensuring proper padding. It processes base64 characters in groups of 4, converting each group back to 3 bytes of binary data.

The decoding algorithm works by:
1. Reading input characters and validating them against the base64 alphabet
2. Accumulating 4 base64 characters (24 bits of data)
3. Extracting 3 bytes from the accumulated 24 bits
4. Handling padding characters ('=') to determine final data length
5. Rejecting invalid characters, whitespace, and malformed sequences

The function includes comprehensive error checking for malformed input and buffer overflow protection.

## Parameters / Member Variables
- `src`: Pointer to the base64-encoded string to decode
- `len`: Length of the source string in characters
- `dst`: Destination buffer for the decoded binary data
- `dstlen`: Size of the destination buffer in bytes

## Dependencies
- Functions called/Symbols referenced:
  - `b64lookup` (static lookup table for base64 character validation)
  - `memset` (for error handling - zeroing destination buffer)
  - `Assert` (for debugging assertions)

- Called from (representative examples):
  - `[scram_verify_plain_password](../s/scram_verify_plain_password.md)` (src/backend/libpq/auth-scram.c:542)
  - `[parse_scram_secret](parse_scram_secret.md)` (src/backend/libpq/auth-scram.c:639, 650, 658)
  - `[read_client_final_message](../r/read_client_final_message.md)` (src/backend/libpq/auth-scram.c:1375)
  - `[read_server_first_message](../r/read_server_first_message.md)` (src/interfaces/libpq/fe-auth-scram.c:655)

## Notes and Other Information
- Returns the length of the decoded data on success, or -1 on error
- On error, the destination buffer is zeroed for security
- Strict validation rejects whitespace characters (space, tab, newline, carriage return)
- Validates proper padding sequence (1 or 2 '=' characters at the end)
- Rejects invalid base64 characters and malformed sequences
- Primarily used in SCRAM authentication for decoding cryptographic data
- Requires properly formatted base64 input without any extraneous characters
- Buffer overflow protection ensures safe operation with pre-allocated buffers

## Simplified Source

```c
int pg_b64_decode(const char *src, int len, char *dst, int dstlen)
{
    const char *srcend = src + len;
    const char *s = src;
    char *p = dst;
    uint32 buf = 0;
    int pos = 0, end = 0;

    while (s < srcend) {
        char c = *s++;

        // Reject whitespace
        if (c == ' ' || c == '\t' || c == '\n' || c == '\r')
            goto error;

        int b;
        if (c == '=') {
            // Handle padding characters
            if (pos == 2) end = 1;
            else if (pos == 3) end = 2;
            else goto error;
            b = 0;
        } else {
            // Look up base64 character value
            if (c <= 0 || c >= 127 || (b = b64lookup[c]) < 0)
                goto error;
        }

        // Accumulate 4 characters into 24-bit buffer
        buf = (buf << 6) + b;
        pos++;

        if (pos == 4) {
            // Extract 3 bytes from 24-bit buffer
            if ((p - dst + 1) > dstlen) goto error;
            *p++ = (buf >> 16) & 255;

            if (end == 0 || end > 1) {
                if ((p - dst + 1) > dstlen) goto error;
                *p++ = (buf >> 8) & 255;
            }
            if (end == 0 || end > 2) {
                if ((p - dst + 1) > dstlen) goto error;
                *p++ = buf & 255;
            }

            buf = 0;
            pos = 0;
        }
    }

    // Ensure proper termination
    if (pos != 0) goto error;

    return p - dst;

error:
    memset(dst, 0, dstlen);
    return -1;
}
```