# PQescapeInternal

## Location
src/interfaces/libpq/fe-exec.c: 4214 - 4364

## Overview
PQescapeInternal is a static internal function that escapes arbitrary strings as either SQL identifiers or literals, returning a newly allocated buffer with proper quoting and escaping.

## Definition


## Detailed Description
PQescapeInternal provides the core escaping functionality for both SQL literals and identifiers in libpq. Unlike PQescapeStringInternal which works with pre-allocated buffers, this function allocates and returns a new buffer containing the properly escaped and quoted string.

The function performs two main phases:
1. **Analysis phase**: Scans the input string to count characters that need escaping and validates multibyte character encoding
2. **Construction phase**: Allocates an appropriately sized buffer and builds the escaped output with proper quoting

For literals, the function handles backslashes by using PostgreSQL's escape string syntax (E'...') when necessary, ensuring compatibility with both standard_conforming_strings settings. For identifiers, it uses double quotes and doubles any embedded quote characters.

The function includes comprehensive multibyte character validation to prevent security issues where invalid sequences could be used to bypass quote characters during parsing.

## Parameters / Member Variables
- : PostgreSQL connection handle (required - function fails if NULL)
- : Input string to be escaped
- : Maximum length of the source string to process
- : Boolean flag determining escape mode (true for identifiers, false for literals)

## Dependencies
- Functions called/Symbols referenced:
  - strnlen
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