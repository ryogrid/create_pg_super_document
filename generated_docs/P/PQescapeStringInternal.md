# PQescapeStringInternal

## Location
src/interfaces/libpq/fe-exec.c: 4071 - 4176

## Overview
PQescapeStringInternal is a static internal function that escapes arbitrary strings to create valid SQL literal strings, handling both single-byte and multibyte character encodings safely.

## Definition


## Detailed Description
PQescapeStringInternal performs the core string escaping functionality for libpq. It converts arbitrary input strings into properly escaped SQL literal strings by:

1. Replacing single quotes (') with doubled single quotes ('')
2. When not using standard strings, replacing backslashes (\) with doubled backslashes (\)
3. Handling multibyte character validation and encoding issues
4. Ensuring the output is safe for SQL parsing

The function processes the input string character by character, using a fast path for plain ASCII characters and a slower validation path for potential multibyte characters. Invalid multibyte sequences are replaced with encoding-specific invalid markers to ensure the escaped string will trigger server-side errors rather than being silently misinterpreted.

## Parameters / Member Variables
- : PostgreSQL connection handle, used for error reporting (can be NULL)
- : Output buffer where the escaped string will be written (must be at least 2*length + 1 bytes)
- : Input string to be escaped
- : Maximum length of the source string to process
- : Pointer to int that will be set to 1 if encoding errors occur (can be NULL)
- : Character encoding identifier for multibyte character validation
- : Boolean indicating whether standard SQL string literals are being used

## Dependencies
- Functions called/Symbols referenced:
  - strnlen
  - IS_HIGHBIT_SET
  - SQL_STR_DOUBLE
  - pg_encoding_mblen_or_incomplete
  - pg_encoding_verifymbchar
  - pg_encoding_set_invalid
  - libpq_append_conn_error
- Called from (representative examples):
  - PQescapeStringConn
  - PQescapeString

## Notes and Other Information
- This is a static internal function not exposed in the public libpq API
- The output buffer must be at least 2*length + 1 bytes to accommodate worst-case escaping
- Invalid multibyte characters are replaced with encoding-specific invalid sequences to ensure safety
- The function stops processing if it encounters a NUL byte before reaching the specified length
- Error reporting is optional - the function can operate without a connection handle or error pointer
- The function ensures the output string is always NUL-terminated regardless of input termination