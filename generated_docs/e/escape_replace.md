# escape_replace

## Location
[src/test/modules/test_escape/test_escape.c:353-378](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_escape/test_escape.c#L353-L378)

## Overview
A static helper function in PostgreSQL's test_escape module that implements a simple string escaping method by replacing single quotes with doubled single quotes, suitable for non-core drivers and validating encoding-valid input.

## Definition
static bool escape_replace(PGconn *conn, PQExpBuffer target, const char *unescaped, size_t unescaped_len, PQExpBuffer escape_err)

## Detailed Description
The escape_replace function implements a straightforward approach to escaping SQL string literals by manually scanning the input string and doubling any single quote characters (replacing ' with ''). This method is commonly used by non-core PostgreSQL drivers that either wrap libpq or implement their own escaping logic. The function works by iterating through each character of the input string, copying regular characters directly to the output buffer and replacing single quotes with two consecutive single quotes. It wraps the entire result in single quotes to form a proper SQL string literal. This approach is sufficient and safe when the input string passes encoding validation, which is why it's marked as supports_only_valid in the test framework.

## Parameters / Member Variables
- conn: PGconn connection object (not used in this implementation)
- target: PQExpBuffer where the escaped string will be appended
- unescaped: Input string to be escaped
- unescaped_len: Length of the unescaped input string
- escape_err: PQExpBuffer for error messages (not used in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - appendPQExpBufferChar
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
- Called from (representative examples):
  - [escape_fmt_id](escape_fmt_id.md)

## Notes and Other Information
This function is part of PostgreSQL's test infrastructure for validating different escape methods. It always returns true as it doesn't perform error checking, unlike other escape functions in the module. The function demonstrates the simplest possible approach to SQL string escaping, which is sufficient for many use cases but lacks the sophisticated encoding and context awareness of libpq's built-in escaping functions. It serves as a reference implementation for how non-core drivers typically handle string escaping when they don't have access to PostgreSQL's internal escaping mechanisms.