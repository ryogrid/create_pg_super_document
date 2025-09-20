# escape_append_literal

## Location
[src/test/modules/test_escape/test_escape.c:379-388](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_escape/test_escape.c#L379-L388)

## Overview
A static helper function that appends a properly escaped string literal to a PQExpBuffer using PostgreSQL's string escaping rules.

## Definition

```c
static bool
escape_append_literal(PGconn *conn, PQExpBuffer target,
					  const char *unescaped, size_t unescaped_len,
					  PQExpBuffer escape_err)
```
## Detailed Description
This function is a wrapper around the  function that formats an unescaped string as a properly quoted and escaped PostgreSQL string literal. It uses the client encoding from the provided database connection to ensure proper character encoding handling. The function always returns , indicating successful operation, as the underlying  function handles all the complexity of string escaping.

## Parameters / Member Variables
- : PostgreSQL database connection handle used to determine client encoding
- : PQExpBuffer to which the escaped string literal will be appended
- : Input string to be escaped and formatted as a literal
- : Length of the unescaped input string (parameter present but not used in current implementation)
- : PQExpBuffer for error messages (parameter present but not used in current implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [appendStringLiteral](../a/appendStringLiteral.md)
  - [PQclientEncoding](../P/PQclientEncoding.md)
- Called from (representative examples):
  - [escape_fmt_id](escape_fmt_id.md)

## Notes and Other Information
- This is a test module function located in 
- The function currently ignores the  and  parameters
- Always returns  indicating successful operation
- Part of the PostgreSQL test escape module for validating string escaping functionality
- The function signature suggests it was designed to handle potential error conditions, but the current implementation doesn't utilize error reporting