# escape_fmt_id

## Location
[src/test/modules/test_escape/test_escape.c:389-445](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_escape/test_escape.c#L389-L445)

## Overview
A static helper function that formats an unescaped string as a properly quoted PostgreSQL identifier using PostgreSQL's identifier escaping rules.

## Definition

```c
static bool
escape_fmt_id(PGconn *conn, PQExpBuffer target,
			  const char *unescaped, size_t unescaped_len,
			  PQExpBuffer escape_err)
```
## Detailed Description
This function formats an unescaped string as a properly quoted PostgreSQL identifier by setting the appropriate encoding context and using the  function. It first sets the formatting encoding to match the client connection's encoding, then applies PostgreSQL's identifier quoting rules to ensure the identifier is properly escaped for safe use in SQL statements. The function always returns , indicating successful operation.

## Parameters / Member Variables
- `*conn`: PostgreSQL database connection handle used to determine client encoding
- `target`: PQExpBuffer to which the escaped identifier will be appended
- `*unescaped`: Input string to be formatted as a PostgreSQL identifier
- `unescaped_len`: Length of the unescaped input string (parameter present but not used in current implementation)
- `escape_err`: PQExpBuffer for error messages (parameter present but not used in current implementation)
## Dependencies
- Functions called/Symbols referenced:
  - [setFmtEncoding](../s/setFmtEncoding.md)
  - [PQclientEncoding](../P/PQclientEncoding.md)
  - [fmtId](../f/fmtId.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
- Called from (representative examples):
  - [escape_fmt_id](escape_fmt_id.md) (recursive self-reference in test context)

## Notes and Other Information
- This is a test module function located in 
- The function currently ignores the  and  parameters
- Always returns  indicating successful operation
- Part of the PostgreSQL test escape module for validating identifier escaping functionality
- Uses the  utility function which handles the actual identifier quoting logic
- The  call ensures proper character encoding handling for the identifier formatting
- Self-references in the test context suggest this function is used recursively in test scenarios

## Simplified Source
```c
static bool
escape_fmt_id(PGconn *conn, PQExpBuffer target,
              const char *unescaped, size_t unescaped_len,
              PQExpBuffer escape_err)
{
    /* Set encoding context for proper identifier formatting */
    setFmtEncoding(PQclientEncoding(conn));

    /* Format and append the escaped identifier */
    appendPQExpBufferStr(target, fmtId(unescaped));

    return true;  /* Always succeeds */
}
```