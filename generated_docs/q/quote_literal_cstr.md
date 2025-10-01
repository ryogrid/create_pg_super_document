# quote_literal_cstr

## Location
[src/backend/utils/adt/quote.c:103-124](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/quote.c#L103-L124)

## Overview
A C-level utility function that takes a null-terminated C string and returns a properly quoted and escaped SQL string literal, suitable for use in dynamically constructed SQL statements.

## Definition

```c
char *
quote_literal_cstr(const char *rawstr)
```
## Detailed Description
The `quote_literal_cstr` function provides a C-level interface for quoting string literals, working directly with null-terminated C strings rather than PostgreSQL's text type. It calculates the length of the input string, allocates sufficient memory for the worst-case scenario (where all characters might need to be doubled plus quotes and null terminator), and delegates the actual quoting work to `quote_literal_internal`. After the quoting is complete, it null-terminates the result string to ensure it's a proper C string. This function is essential for internal PostgreSQL code that needs to safely incorporate string values into SQL queries.

## Parameters / Member Variables
- `rawstr`: A null-terminated C string to be quoted as a SQL literal

## Dependencies
- Functions called/Symbols referenced:
  - [quote_literal_internal](quote_literal_internal.md) - Core function that performs the actual quoting and escaping logic
- Called from (representative examples):
  - [get_publications_str](../g/get_publications_str.md) - Function for formatting publication names in subscription commands
  - [fetch_remote_table_info](../f/fetch_remote_table_info.md) - Function in table synchronization for quoting table and schema names
  - [text_format_string_conversion](../t/text_format_string_conversion.md) - Function in string formatting utilities
  - [PLy_quote_literal](../P/PLy_quote_literal.md) - Python language extension function for literal quoting
  - Various replication and logical synchronization functions

## Notes and Other Information
- Returns a newly allocated C string that must be freed by the caller
- Allocates memory for worst-case scenario (all characters doubled plus quotes and null terminator)
- Used extensively in PostgreSQL's internal C code for safe SQL construction
- Part of PostgreSQL's quote utility functions located in `src/backend/utils/adt/quote.c`
- Critical for preventing SQL injection in internal PostgreSQL operations
- Widely used in replication, table synchronization, and language extension modules
- The returned string is null-terminated, making it safe for use with standard C string functions

## Simplified Source

```c
char *
quote_literal_cstr(const char *rawstr)
{
    char *result;
    int   len;
    int   newlen;

    // Get input string length
    len = strlen(rawstr);

    // Allocate worst-case buffer (all chars doubled + quotes + null terminator)
    result = palloc(len * 2 + 3 + 1);

    // Quote the literal
    newlen = quote_literal_internal(result, rawstr, len);

    // Null-terminate the result
    result[newlen] = '\0';

    return result;
}
```