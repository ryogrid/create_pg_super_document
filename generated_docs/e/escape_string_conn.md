# escape_string_conn

## Location
[src/test/modules/test_escape/test_escape.c:299-329](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_escape/test_escape.c#L299-L329)

## Overview
A static helper function in PostgreSQL's test_escape module that wraps the PQescapeStringConn function to escape string values for SQL queries, manually adding surrounding single quotes and handling buffer management.

## Definition
static bool escape_string_conn(PGconn *conn, PQExpBuffer target, const char *unescaped, size_t unescaped_len, PQExpBuffer escape_err)

## Detailed Description
The escape_string_conn function provides a wrapper around libpq's PQescapeStringConn function to safely escape string values for inclusion in SQL statements. Unlike escape_literal which uses PQescapeLiteral, this function manually constructs the escaped string by adding single quotes around the content and using PQescapeStringConn to escape the inner content. It pre-allocates buffer space for efficiency and handles error reporting through PQExpBuffer structures. The function ensures that string values are properly escaped according to PostgreSQL's string literal rules, preventing SQL injection vulnerabilities.

## Parameters / Member Variables
- conn: PGconn connection object used for escaping context
- target: PQExpBuffer where the escaped string will be appended
- unescaped: Input string to be escaped
- unescaped_len: Length of the unescaped input string
- escape_err: PQExpBuffer to store error messages if escaping fails

## Dependencies
- Functions called/Symbols referenced:
  - [appendPQExpBufferChar](../a/appendPQExpBufferChar.md)
  - [enlargePQExpBuffer](enlargePQExpBuffer.md)
  - [PQescapeStringConn](../P/PQescapeStringConn.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [PQerrorMessage](../P/PQerrorMessage.md)
- Called from (representative examples):
  - [escape_fmt_id](escape_fmt_id.md)

## Notes and Other Information
This function is part of PostgreSQL's test infrastructure for validating escape functionality. It returns true on success and false on failure, with error details stored in the escape_err buffer. The function manually manages the single quote delimiters and pre-enlarges the buffer to accommodate the worst-case scenario where every character needs escaping (unescaped_len * 2 + 1). This approach provides more direct control over the escaping process compared to the higher-level PQescapeLiteral function used in escape_literal.