# esc_decode

## Location
[src/backend/utils/adt/encode.c:454-501](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/encode.c#L454-L501)

## Overview
Decodes escape sequence encoded data back to its original binary format, reversing the encoding performed by esc_encode.

## Definition
```c
static uint64 esc_decode(const char *src, size_t srclen, char *dst)
```

## Detailed Description
This function performs the reverse operation of esc_encode, converting escaped textual representation back to binary data. The decoding rules are:

1. Octal escape sequences (\\nnn where nnn are 3 octal digits 000-377) are converted back to their original byte values
2. Double backslashes (\\\\) are converted back to single backslashes (\\)
3. All other characters are copied unchanged
4. Invalid escape sequences trigger an error

The function validates escape sequences during decoding and reports errors for malformed input, ensuring data integrity during the decode process.

## Parameters / Member Variables
- `src`: Input buffer containing escape-encoded data to be decoded
- `srclen`: Length of the encoded input data in bytes
- `dst`: Output buffer where decoded binary data will be written (must be pre-allocated)

## Dependencies
- Functions called/Symbols referenced:
  - VAL (macro to convert octal character to numeric value)
  - ereport (error reporting function)
  - [errcode](errcode.md) (error code function)
  - [errmsg](errmsg.md) (error message function)
- Called from (representative examples):
  - [esc_dec_len](esc_dec_len.md) (indirectly referenced)

## Notes and Other Information
- This is a static utility function used internally within PostgreSQL's encoding system
- The function returns the actual length of the decoded output
- Validates octal escape sequences: first digit must be 0-3, remaining digits 0-7
- Throws ERROR with ERRCODE_INVALID_TEXT_REPRESENTATION for invalid escape sequences
- The decoding process is the exact inverse of esc_encode
- Used primarily for bytea data type processing and other binary data handling
- Caller must ensure destination buffer is sufficiently sized for decoded output

## Simplified Source

```c
static uint64 esc_decode(const char *src, size_t srclen, char *dst) {
    const char *src_end = src + srclen;
    char *output_ptr = dst;
    uint64 output_len = 0;

    while (src < src_end) {
        if (src[0] != '\\') {
            // Regular character -> copy as-is
            *output_ptr++ = *src++;
        }
        else if (src + 3 < src_end &&
                 (src[1] >= '0' && src[1] <= '3') &&
                 (src[2] >= '0' && src[2] <= '7') &&
                 (src[3] >= '0' && src[3] <= '7')) {
            // Octal escape sequence \nnn -> convert to byte
            int byte_val = VAL(src[1]);
            byte_val = (byte_val << 3) + VAL(src[2]);
            byte_val = (byte_val << 3) + VAL(src[3]);
            *output_ptr++ = byte_val;
            src += 4;
        }
        else if (src + 1 < src_end && src[1] == '\\') {
            // Double backslash -> single backslash
            *output_ptr++ = '\\';
            src += 2;
        }
        else {
            // Invalid escape sequence
            ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                           errmsg("invalid input syntax for type %s", "bytea")));
        }
        output_len++;
    }

    return output_len;
}
```