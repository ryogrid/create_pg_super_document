# pset_quoted_string

## Location
[src/bin/psql/command.c:5155-5192](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L5155-L5192)

## Overview
Creates a properly quoted and escaped string suitable for display in psql output, handling special characters like newlines and single quotes.

## Definition

```c
static char *
pset_quoted_string(const char *str)
```
## Detailed Description
The pset_quoted_string function creates a quoted version of an input string by wrapping it in single quotes and escaping special characters within the string. It specifically handles two types of characters that require escaping: newlines (converted to \n) and single quotes (escaped with backslashes). The function allocates sufficient memory to accommodate the worst-case scenario where every character might need escaping, plus space for the surrounding quotes and null terminator.

This function is essential for displaying string values in psql settings output, ensuring that special characters are properly represented and that the output remains parseable and visually clear.

## Parameters / Member Variables
- : Input string to be quoted and escaped

## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc](pg_malloc.md) (for memory allocation)
  - strlen (implicitly used for memory calculation)
- Called from (representative examples):
  - [pset_value_string](pset_value_string.md) (multiple calls)

## Notes and Other Information
- Allocates memory equal to strlen(str) * 2 + 3 to handle worst-case escaping
- Converts newline characters (\n) to the literal string "\n"
- Escapes single quotes by preceding them with backslashes
- Returns a newly allocated string that must be freed by the caller
- Static function scope limits its usage to within command.c
- Used primarily for formatting string settings in psql's \pset command output
- The returned string is always wrapped in single quotes for consistent formatting