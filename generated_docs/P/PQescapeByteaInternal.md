# PQescapeByteaInternal

## Location
[src/interfaces/libpq/fe-exec.c:4418-4513](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L4418-L4513)

## Overview
PQescapeByteaInternal converts binary data to a string representation suitable for inclusion in SQL statements as bytea literals, supporting both hexadecimal and traditional escape formats.

## Definition

```c
static unsigned char *
PQescapeByteaInternal(PGconn *conn,
					  const unsigned char *from, size_t from_length,
					  size_t *to_length, bool std_strings, bool use_hex)
```
## Detailed Description
PQescapeByteaInternal is the core implementation function for bytea escaping in libpq. It supports two encoding formats: hexadecimal (\\x followed by hex digits) and traditional escape format (using octal sequences for non-printable characters). In escape mode, it applies these transformations: null bytes become \\000, single quotes are doubled, backslashes are escaped appropriately based on standard_conforming_strings setting, and non-printable characters (< 0x20 or > 0x7e) become octal escape sequences (\\ooo). The function calculates the required output buffer size, allocates memory, and performs the encoding while handling the differences between standard and non-standard string modes.

## Parameters / Member Variables
- `*conn`: PostgreSQL connection handle for error reporting (may be NULL)
- `*from`: Source binary data to be escaped
- `from_length`: Length of the source data in bytes
- `*to_length`: Pointer to store the length of the resulting escaped string
- `std_strings`: Whether standard_conforming_strings is enabled (affects backslash doubling)
- `use_hex`: If true, use hexadecimal format; if false, use traditional escape format
## Dependencies
- Functions called/Symbols referenced:
  - malloc
  - hextbl (static hex character lookup table)
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md) (for error reporting)
- Called from (representative examples):
  - [PQescapeByteaConn](PQescapeByteaConn.md)
  - [PQescapeBytea](PQescapeBytea.md)

## Notes and Other Information
- Returns a newly allocated string that must be freed by the caller
- Returns NULL on memory allocation failure, with error stored in connection if provided
- Hex format produces shorter output for most binary data (\\x + 2 chars per byte)
- Traditional escape format uses octal sequences (\\nnn) for non-printable characters
- Handles standard_conforming_strings setting correctly for backslash escaping
- Essential for safely embedding binary data in PostgreSQL SQL statements
- Used internally by the public PQescapeBytea functions to provide bytea escaping functionality

## Simplified Source

```c
static unsigned char *
PQescapeByteaInternal(PGconn *conn, const unsigned char *from, size_t from_length,
                      size_t *to_length, bool std_strings, bool use_hex) {
    size_t len = 1; // Start with space for null terminator
    size_t bslash_len = (std_strings ? 1 : 2);

    // Calculate required output length
    if (use_hex) {
        len += bslash_len + 1 + 2 * from_length; // \x + 2 chars per byte
    } else {
        // Traditional escape: count special chars needing escaping
        for (size_t i = 0; i < from_length; i++) {
            unsigned char c = from[i];
            if (c < 0x20 || c > 0x7e)
                len += bslash_len + 3; // \ooo octal
            else if (c == '\'')
                len += 2; // Double quotes
            else if (c == '\\')
                len += bslash_len + bslash_len; // Escape backslashes
            else
                len++; // Regular character
        }
    }

    // Allocate output buffer
    *to_length = len;
    unsigned char *result = malloc(len);
    if (!result) {
        if (conn) libpq_append_conn_error(conn, "out of memory");
        return NULL;
    }

    unsigned char *rp = result;

    // Add hex prefix if using hex format
    if (use_hex) {
        if (!std_strings) *rp++ = '\\';
        *rp++ = '\\';
        *rp++ = 'x';
    }

    // Encode each byte
    for (size_t i = 0; i < from_length; i++) {
        unsigned char c = from[i];

        if (use_hex) {
            // Hex encoding: convert to two hex digits
            *rp++ = hextbl[(c >> 4) & 0xF];
            *rp++ = hextbl[c & 0xF];
        } else if (c < 0x20 || c > 0x7e) {
            // Non-printable: use octal escape \ooo
            if (!std_strings) *rp++ = '\\';
            *rp++ = '\\';
            *rp++ = (c >> 6) + '0';
            *rp++ = ((c >> 3) & 07) + '0';
            *rp++ = (c & 07) + '0';
        } else if (c == '\'') {
            // Escape single quotes by doubling
            *rp++ = '\'';
            *rp++ = '\'';
        } else if (c == '\\') {
            // Escape backslashes
            if (!std_strings) {
                *rp++ = '\\';
                *rp++ = '\\';
            }
            *rp++ = '\\';
            *rp++ = '\\';
        } else {
            // Regular printable character
            *rp++ = c;
        }
    }

    *rp = '\0';
    return result;
}
```