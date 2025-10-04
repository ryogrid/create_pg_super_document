# PQescapeInternal

## Location
[src/interfaces/libpq/fe-exec.c:4214-4364](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L4214-L4364)

## Overview
PQescapeInternal is a static internal function that escapes arbitrary strings as either SQL identifiers or literals, returning a newly allocated buffer with proper quoting and escaping.

## Definition

```c
static char *
PQescapeInternal(PGconn *conn, const char *str, size_t len, bool as_ident)
```
## Detailed Description
PQescapeInternal provides the core escaping functionality for both SQL literals and identifiers in libpq. Unlike PQescapeStringInternal which works with pre-allocated buffers, this function allocates and returns a new buffer containing the properly escaped and quoted string.

The function performs two main phases:
1. **Analysis phase**: Scans the input string to count characters that need escaping and validates multibyte character encoding
2. **Construction phase**: Allocates an appropriately sized buffer and builds the escaped output with proper quoting

For literals, the function handles backslashes by using PostgreSQL's escape string syntax (E'...') when necessary, ensuring compatibility with both standard_conforming_strings settings. For identifiers, it uses double quotes and doubles any embedded quote characters.

The function includes comprehensive multibyte character validation to prevent security issues where invalid sequences could be used to bypass quote characters during parsing.

## Parameters / Member Variables
- `*conn`: PostgreSQL connection handle (required - function fails if NULL)
- `*str`: Input string to be escaped
- `len`: Maximum length of the source string to process
- `as_ident`: Boolean flag determining escape mode (true for identifiers, false for literals)
## Dependencies
- Functions called/Symbols referenced:
  - [strnlen](../s/strnlen.md)
  - pqClearConnErrorState
  - IS_HIGHBIT_SET
  - [pg_encoding_mblen_or_incomplete](../p/pg_encoding_mblen_or_incomplete.md)
  - [pg_encoding_verifymbstr](../p/pg_encoding_verifymbstr.md)
  - malloc
  - [pg_encoding_mblen](../p/pg_encoding_mblen.md)
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md)
  - memcpy
- Called from (representative examples):
  - [PQescapeLiteral](PQescapeLiteral.md)
  - [PQescapeIdentifier](PQescapeIdentifier.md)

## Notes and Other Information
- This is a static internal function not exposed in the public libpq API
- Returns NULL on failure (encoding errors or out of memory) with error details stored in connection
- Allocates memory that must be freed by the caller using PQfreemem()
- Uses different quote characters: single quotes (') for literals, double quotes (") for identifiers
- For literals containing backslashes, generates escape string syntax (E'...') with leading space
- Performs complete multibyte character validation on first encounter of high-bit-set characters
- Uses optimized fast path when no special characters need escaping
- Always returns a properly quoted and NUL-terminated string
- Clears connection error state before processing when no commands are queued

## Simplified Source

```c
static char *PQescapeInternal(PGconn *conn, const char *str, size_t len, bool as_ident) {
    size_t input_len = strnlen(str, len);
    char quote_char = as_ident ? '"' : '\'';
    int num_quotes = 0;
    int num_backslashes = 0;
    bool validated_mb = false;

    // Validate connection
    if (!conn)
        return NULL;

    // Clear error state if no pending commands
    if (conn->cmd_queue_head == NULL)
        pqClearConnErrorState(conn);

    // Scan string to count special characters and validate encoding
    const char *s = str;
    for (size_t remaining = input_len; remaining > 0; remaining--, s++) {
        if (*s == quote_char)
            ++num_quotes;
        else if (*s == '\\')
            ++num_backslashes;
        else if (IS_HIGHBIT_SET(*s)) {
            // Handle multibyte characters
            int charlen = pg_encoding_mblen_or_incomplete(conn->client_encoding, s, remaining);

            if (charlen > remaining) {
                libpq_append_conn_error(conn, "incomplete multibyte character");
                return NULL;
            }

            // Validate multibyte characters once
            if (!validated_mb) {
                if (pg_encoding_verifymbstr(conn->client_encoding, s, remaining) != remaining) {
                    libpq_append_conn_error(conn, "invalid multibyte character");
                    return NULL;
                }
                validated_mb = true;
            }

            s += charlen - 1;
            remaining -= charlen - 1;
        }
    }

    // Calculate buffer size and allocate
    size_t result_size = input_len + num_quotes + 3; // quotes + NUL
    if (!as_ident && num_backslashes > 0)
        result_size += num_backslashes + 2; // for E'...' syntax

    char *result = malloc(result_size);
    if (!result) {
        libpq_append_conn_error(conn, "out of memory");
        return NULL;
    }

    char *rp = result;

    // Add escape string prefix for literals with backslashes
    if (!as_ident && num_backslashes > 0) {
        *rp++ = ' ';
        *rp++ = 'E';
    }

    // Opening quote
    *rp++ = quote_char;

    // Copy content with escaping
    if (num_quotes == 0 && (num_backslashes == 0 || as_ident)) {
        // Fast path: direct copy
        memcpy(rp, str, input_len);
        rp += input_len;
    } else {
        // Slow path: character-by-character with escaping
        s = str;
        for (size_t remaining = input_len; remaining > 0; remaining--, s++) {
            if (*s == quote_char || (!as_ident && *s == '\\')) {
                // Double the special character
                *rp++ = *s;
                *rp++ = *s;
            } else if (!IS_HIGHBIT_SET(*s)) {
                *rp++ = *s;
            } else {
                // Copy multibyte character
                int mblen = pg_encoding_mblen(conn->client_encoding, s);
                for (int i = 0; i < mblen; i++) {
                    *rp++ = *s++;
                    remaining--;
                }
                s--; // Adjust for loop increment
                remaining++;
            }
        }
    }

    // Closing quote and terminator
    *rp++ = quote_char;
    *rp = '\0';

    return result;
}
```