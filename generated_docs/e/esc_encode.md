# esc_encode

## Location
[src/backend/utils/adt/encode.c:415-453](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/encode.c#L415-L453)

## Overview
Encodes binary data using PostgreSQL's escape sequence format, converting null bytes, high-bit characters, and backslashes to their escaped representations.

## Definition
```c
static uint64 esc_encode(const char *src, size_t srclen, char *dst)
```

## Detailed Description
This function performs escape sequence encoding on binary data, transforming potentially problematic characters into a safe textual representation. The encoding rules are:

1. Null bytes (\0) and high-bit characters (>= 128) are converted to octal escape sequences in the format \\nnn
2. Backslash characters (\\) are doubled to \\\\
3. All other characters are copied unchanged

The encoding ensures that binary data can be safely represented as text and later decoded back to its original form. This is particularly useful for storing binary data in text-based contexts.

## Parameters / Member Variables
- `src`: Input buffer containing binary data to be encoded
- `srclen`: Length of the input data in bytes
- `dst`: Output buffer where encoded data will be written (must be pre-allocated with sufficient size)

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET (macro to check if high bit is set)
  - DIG (macro to convert digit to character for octal representation)
- Called from (representative examples):
  - [esc_dec_len](esc_dec_len.md) (indirectly referenced)

## Notes and Other Information
- This is a static utility function used internally within PostgreSQL's encoding system
- The function returns the actual length of the encoded output
- Caller must ensure destination buffer is large enough (use esc_enc_len to calculate required size)
- Octal escape sequences use exactly 4 characters: backslash followed by 3 octal digits
- The encoding is deterministic and reversible through corresponding decode functions
- Used as part of PostgreSQL's bytea data type handling and other binary data operations

## Simplified Source

```c
static uint64 esc_encode(const char *src, size_t srclen, char *dst) {
    const char *src_end = src + srclen;
    char *output_ptr = dst;
    uint64 output_len = 0;

    while (src < src_end) {
        unsigned char ch = (unsigned char) *src;

        if (ch == '\0' || IS_HIGHBIT_SET(ch)) {
            // Null bytes and high-bit chars -> \nnn octal escape
            output_ptr[0] = '\\';
            output_ptr[1] = DIG(ch >> 6);       // High 3 bits
            output_ptr[2] = DIG((ch >> 3) & 7); // Middle 3 bits
            output_ptr[3] = DIG(ch & 7);        // Low 3 bits
            output_ptr += 4;
            output_len += 4;
        }
        else if (ch == '\\') {
            // Backslash -> double backslash
            output_ptr[0] = '\\';
            output_ptr[1] = '\\';
            output_ptr += 2;
            output_len += 2;
        }
        else {
            // Regular character -> copy as-is
            *output_ptr++ = ch;
            output_len++;
        }
        src++;
    }

    return output_len;
}
```