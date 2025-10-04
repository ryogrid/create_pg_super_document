# quote_postgres

## Location
[src/interfaces/ecpg/ecpglib/execute.c:40-82](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/execute.c#L40-L82)

## Overview
A static utility function in ECPG that escapes and quotes strings for safe inclusion in PostgreSQL SQL statements.

## Definition

```c
static char *
quote_postgres(char *arg, bool quote, int lineno)
```
## Detailed Description
The  function handles string escaping and quoting for ECPG (Embedded SQL in C for PostgreSQL). When  is true, it creates a properly escaped and quoted string literal that can be safely inserted into SQL statements. The function uses PostgreSQL's  to handle special characters like single quotes and backslashes, and automatically determines whether to use standard string literals or escape string syntax (E'...' format) based on the escaping results.

When  is false, the function simply returns the original string unchanged, as the quoting will be handled later when the string is inserted into a statement.

## Parameters / Member Variables
- `*arg`: Input string to be quoted and escaped
- `quote`: Boolean flag indicating whether quoting should be performed
- `lineno`: Line number for memory allocation tracking (used by ecpg_alloc)
## Dependencies
- Functions called/Symbols referenced:
  - [ecpg_alloc](../e/ecpg_alloc.md)
  - [PQescapeString](../P/PQescapeString.md)
  - ESCAPE_STRING_SYNTAX
  - [ecpg_free](../e/ecpg_free.md)
- Called from (representative examples):
  - [ecpg_store_input](../e/ecpg_store_input.md) (multiple locations)

## Notes and Other Information
- The function always uses E'' (escape string) syntax when characters were escaped to ensure compatibility regardless of the target database's standard_conforming_strings setting
- Memory management is handled through ECPG's allocation functions (ecpg_alloc/ecpg_free)
- The original input string is freed when quoting is performed
- Buffer allocation accounts for worst-case escaping (2x original length plus quotes and null terminator)

## Simplified Source

```c
static char *
quote_postgres(char *arg, bool quote, int lineno)
{
    // If no quoting requested, return original string
    if (!quote)
        return arg;

    // Allocate buffer for escaped string with quotes
    size_t length = strlen(arg);
    size_t buffer_len = 2 * length + 1;
    char *res = ecpg_alloc(buffer_len + 3, lineno);
    if (!res)
        return res;

    // Escape string using PostgreSQL's escape function
    size_t escaped_len = PQescapeString(res + 1, arg, buffer_len);

    if (length == escaped_len) {
        // No escaping needed - use standard quotes
        res[0] = res[escaped_len + 1] = '\'';
        res[escaped_len + 2] = '\0';
    } else {
        // Escaping occurred - use E'' syntax for compatibility
        memmove(res + 2, res + 1, escaped_len);
        res[0] = ESCAPE_STRING_SYNTAX;  // 'E'
        res[1] = res[escaped_len + 2] = '\'';
        res[escaped_len + 3] = '\0';
    }

    // Free original string and return escaped version
    ecpg_free(arg);
    return res;
}
```