# PQescapeStringInternal

## Location
[src/interfaces/libpq/fe-exec.c:4071-4176](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L4071-L4176)

## Overview
PQescapeStringInternal is a static internal function that escapes arbitrary strings to create valid SQL literal strings, handling both single-byte and multibyte character encodings safely.

## Definition

```c
static size_t
PQescapeStringInternal(PGconn *conn,
					   char *to, const char *from, size_t length,
					   int *error,
					   int encoding, bool std_strings)
```
## Detailed Description
PQescapeStringInternal performs the core string escaping functionality for libpq. It converts arbitrary input strings into properly escaped SQL literal strings by:

1. Replacing single quotes (') with doubled single quotes ('')
2. When not using standard strings, replacing backslashes (\) with doubled backslashes (\)
3. Handling multibyte character validation and encoding issues
4. Ensuring the output is safe for SQL parsing

The function processes the input string character by character, using a fast path for plain ASCII characters and a slower validation path for potential multibyte characters. Invalid multibyte sequences are replaced with encoding-specific invalid markers to ensure the escaped string will trigger server-side errors rather than being silently misinterpreted.

## Parameters / Member Variables
- `*conn`: PostgreSQL connection handle, used for error reporting (can be NULL)
- `*to`: Output buffer where the escaped string will be written (must be at least 2*length + 1 bytes)
- `*from`: Input string to be escaped
- `length`: Maximum length of the source string to process
- `*error`: Pointer to int that will be set to 1 if encoding errors occur (can be NULL)
- `encoding`: Character encoding identifier for multibyte character validation
- `std_strings`: Boolean indicating whether standard SQL string literals are being used
## Dependencies
- Functions called/Symbols referenced:
  - [strnlen](../s/strnlen.md)
  - IS_HIGHBIT_SET
  - SQL_STR_DOUBLE
  - [pg_encoding_mblen_or_incomplete](../p/pg_encoding_mblen_or_incomplete.md)
  - [pg_encoding_verifymbchar](../p/pg_encoding_verifymbchar.md)
  - [pg_encoding_set_invalid](../p/pg_encoding_set_invalid.md)
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md)
- Called from (representative examples):
  - [PQescapeStringConn](PQescapeStringConn.md)
  - [PQescapeString](PQescapeString.md)

## Notes and Other Information
- This is a static internal function not exposed in the public libpq API
- The output buffer must be at least 2*length + 1 bytes to accommodate worst-case escaping
- Invalid multibyte characters are replaced with encoding-specific invalid sequences to ensure safety
- The function stops processing if it encounters a NUL byte before reaching the specified length
- Error reporting is optional - the function can operate without a connection handle or error pointer
- The function ensures the output string is always NUL-terminated regardless of input termination

## Simplified Source

```c
static size_t PQescapeStringInternal(PGconn *conn,
                                   char *to, const char *from, size_t length,
                                   int *error,
                                   int encoding, bool std_strings) {
    const char *source = from;
    char *target = to;
    size_t remaining = strnlen(from, length);
    bool already_complained = false;

    if (error)
        *error = 0;

    while (remaining > 0) {
        char c = *source;

        // Fast path for plain ASCII characters
        if (!IS_HIGHBIT_SET(c)) {
            // Apply quoting if needed (single quotes, backslashes)
            if (SQL_STR_DOUBLE(c, !std_strings))
                *target++ = c;
            *target++ = c;
            source++;
            remaining--;
            continue;
        }

        // Slow path for multibyte characters
        int charlen = pg_encoding_mblen_or_incomplete(encoding, source, remaining);

        // Check for invalid multibyte characters
        if (remaining < charlen ||
            pg_encoding_verifymbchar(encoding, source, charlen) == -1) {

            // Handle invalid multibyte sequence
            if (error)
                *error = 1;
            if (conn && !already_complained) {
                libpq_append_conn_error(conn, "invalid multibyte character");
                already_complained = true;
            }

            // Replace with encoding-specific invalid sequence
            pg_encoding_set_invalid(encoding, target);
            target += 2;
            source++;
            remaining--;
        } else {
            // Copy valid multibyte character
            for (int i = 0; i < charlen; i++) {
                *target++ = *source++;
                remaining--;
            }
        }
    }

    // Null-terminate the result
    *target = '\0';
    return target - to;
}
```