# escape_identifier

## Location
[src/test/modules/test_escape/test_escape.c:275-298](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_escape/test_escape.c#L275-L298)

## Overview
A static helper function in PostgreSQL's test_escape module that wraps the PQescapeIdentifier function to escape SQL identifiers with proper error handling and buffer management.

## Definition
static bool escape_identifier(PGconn *conn, PQExpBuffer target, const char *unescaped, size_t unescaped_len, PQExpBuffer escape_err)

## Detailed Description
The escape_identifier function provides a wrapper around libpq's PQescapeIdentifier function to safely escape SQL identifiers such as table names, column names, and other database object names. It handles memory management and error reporting using PQExpBuffer structures. The function ensures that identifiers are properly quoted with double quotes and escaped according to PostgreSQL's SQL identifier rules, preventing SQL injection vulnerabilities and syntax errors when constructing dynamic queries with user-provided identifier names.

## Parameters / Member Variables
- conn: PGconn connection object used for escaping context
- target: PQExpBuffer where the escaped identifier will be appended
- unescaped: Input string to be escaped as an identifier
- unescaped_len: Length of the unescaped input string
- escape_err: PQExpBuffer to store error messages if escaping fails

## Dependencies
- Functions called/Symbols referenced:
  - [PQescapeIdentifier](../P/PQescapeIdentifier.md)
  - [PQfreemem](../P/PQfreemem.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [PQerrorMessage](../P/PQerrorMessage.md)
- Called from (representative examples):
  - [escape_fmt_id](escape_fmt_id.md)

## Notes and Other Information
This function is part of PostgreSQL's test infrastructure for validating escape functionality. It returns true on success and false on failure, with error details stored in the escape_err buffer. The function properly manages memory by freeing the escaped string returned by PQescapeIdentifier using PQfreemem. When an error occurs, it strips the trailing newline from the error message for cleaner formatting. Unlike string literals, identifiers are escaped with double quotes and follow different escaping rules suitable for database object names.

## Simplified Source

```c
static bool
escape_identifier(PGconn *conn, PQExpBuffer target,
                  const char *unescaped, size_t unescaped_len,
                  PQExpBuffer escape_err)
{
    char *escaped;

    // Escape the identifier using libpq function
    escaped = PQescapeIdentifier(conn, unescaped, unescaped_len);

    if (!escaped) {
        // Handle escaping failure - append error message
        appendPQExpBuffer(escape_err, "%s", PQerrorMessage(conn));
        // Remove trailing newline for cleaner formatting
        escape_err->data[escape_err->len - 1] = 0;
        escape_err->len--;
        return false;
    } else {
        // Success - append escaped identifier and cleanup
        appendPQExpBufferStr(target, escaped);
        PQfreemem(escaped);
        return true;
    }
}
```