# escape_literal

## Location
src/test/modules/test_escape/test_escape.c: 251 - 274

## Overview
A static helper function in PostgreSQL's test_escape module that wraps the PQescapeLiteral function to escape string literals for SQL queries with proper error handling and buffer management.

## Definition
static bool escape_literal(PGconn *conn, PQExpBuffer target, const char *unescaped, size_t unescaped_len, PQExpBuffer escape_err)

## Detailed Description
The escape_literal function provides a wrapper around libpq's PQescapeLiteral function to safely escape string literals for inclusion in SQL statements. It handles memory management and error reporting by using PQExpBuffer structures for both the output and error messages. The function ensures that string literals are properly quoted and escaped according to PostgreSQL's SQL syntax rules, preventing SQL injection vulnerabilities when constructing dynamic queries.

## Parameters / Member Variables
- conn: PGconn connection object used for escaping context
- target: PQExpBuffer where the escaped literal will be appended
- unescaped: Input string to be escaped as a literal
- unescaped_len: Length of the unescaped input string
- escape_err: PQExpBuffer to store error messages if escaping fails

## Dependencies
- Functions called/Symbols referenced:
  - [PQescapeLiteral](../P/PQescapeLiteral.md)
  - [PQfreemem](../P/PQfreemem.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [PQerrorMessage](../P/PQerrorMessage.md)
- Called from (representative examples):
  - [escape_fmt_id](escape_fmt_id.md)

## Notes and Other Information
This function is part of PostgreSQL's test infrastructure for validating escape functionality. It returns true on success and false on failure, with error details stored in the escape_err buffer. The function properly manages memory by freeing the escaped string returned by PQescapeLiteral using PQfreemem. When an error occurs, it strips the trailing newline from the error message for cleaner formatting.