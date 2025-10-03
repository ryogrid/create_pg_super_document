# hex_decode_safe

## Location
[src/backend/utils/adt/encode.c:196-236](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/encode.c#L196-L236)

## Overview
Safely decodes hexadecimal-encoded data into binary format with error context support, skipping whitespace characters and validating hexadecimal digit pairs.

## Definition

```c
uint64
hex_decode_safe(const char *src, size_t len, char *dst, Node *escontext)
```
## Detailed Description
This function decodes a hexadecimal-encoded string into binary data with comprehensive error handling. It processes the input string character by character, skipping whitespace (space, newline, tab, carriage return), and converts pairs of hexadecimal digits into single bytes. The function uses PostgreSQL's error context system for soft error handling, allowing callers to handle errors gracefully rather than causing transaction aborts.

The decoding process validates that:
- Each character (excluding whitespace) is a valid hexadecimal digit (0-9, A-F, a-f)
- The total number of hexadecimal digits is even (complete pairs)

Each pair of hexadecimal digits is converted to a single byte by combining the high nibble (first digit shifted left 4 bits) with the low nibble (second digit).

## Parameters / Member Variables
- `*src`: Pointer to the source hexadecimal string to decode
- `len`: Length of the source string in bytes
- `*dst`: Pointer to the destination buffer where decoded binary data will be written
- `*escontext`: Error context node for soft error handling (can be NULL for hard errors)
## Dependencies
- Functions called/Symbols referenced:
  - [get_hex](../g/get_hex.md) (helper function to convert single hex character to value)
  - ereturn (error reporting macro with context support)
  - [pg_mblen](../p/pg_mblen.md) (multibyte character length function for error messages)
- Called from (representative examples):
  - [hex_decode](hex_decode.md) (wrapper function for traditional error handling)
  - [byteain](../b/byteain.md) (bytea input function for processing hex-encoded input)

## Notes and Other Information
- Returns the number of bytes written to the destination buffer (uint64)
- Uses soft error handling via escontext, allowing callers to catch and handle errors
- Automatically skips common whitespace characters for flexible input formatting
- Validates input thoroughly to prevent buffer overruns and invalid data
- Part of PostgreSQL's encoding/decoding subsystem located in src/backend/utils/adt/encode.c
- The function is designed to be memory-safe and handles multibyte character boundaries in error messages

## Simplified Source

```c
uint64
hex_decode_safe(const char *src, size_t len, char *dst, Node *escontext)
{
    const char *s, *srcend;
    char v1, v2, *p;

    srcend = src + len;
    s = src;
    p = dst;

    while (s < srcend)
    {
        // Skip whitespace characters
        if (*s == ' ' || *s == '\n' || *s == '\t' || *s == '\r')
        {
            s++;
            continue;
        }

        // Get first hex digit
        if (!get_hex(s, &v1))
            ereturn(escontext, 0,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("invalid hexadecimal digit: \"%.*s\"",
                            pg_mblen(s), s)));
        s++;

        // Check for complete pair
        if (s >= srcend)
            ereturn(escontext, 0,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("invalid hexadecimal data: odd number of digits")));

        // Get second hex digit
        if (!get_hex(s, &v2))
            ereturn(escontext, 0,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("invalid hexadecimal digit: \"%.*s\"",
                            pg_mblen(s), s)));
        s++;

        // Combine high and low nibbles into byte
        *p++ = (v1 << 4) | v2;
    }

    return p - dst;  // Return number of decoded bytes
}
```