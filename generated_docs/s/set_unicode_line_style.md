# set_unicode_line_style

## Location
[src/bin/psql/command.c:4490-4502](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L4490-L4502)

## Overview
A utility function in psql that parses and validates Unicode line style strings, converting them to the appropriate enumerated values for table border formatting.

## Definition
```c
static bool set_unicode_line_style(const char *value, size_t vallen, unicode_linestyle *linestyle)
```

## Detailed Description
The `set_unicode_line_style` function serves as a parser and validator for Unicode line style settings in psql's table formatting system. It accepts string input representing line style preferences and converts them to the corresponding enum values. The function supports two Unicode line styles: "single" for single-line borders and "double" for double-line borders in table output. The function performs case-insensitive string comparison and updates the provided linestyle parameter only if the input is valid.

## Parameters / Member Variables
- `value`: Pointer to the string containing the line style name to be parsed
- `vallen`: Length of the value string to be compared
- `linestyle`: Pointer to a unicode_linestyle enum variable that will be updated with the parsed value

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strncasecmp](../p/pg_strncasecmp.md) (PostgreSQL case-insensitive string comparison function)
  - [unicode_linestyle](../u/unicode_linestyle.md) (enum type for line styles)
  - UNICODE_LINESTYLE_SINGLE, UNICODE_LINESTYLE_DOUBLE (enum values)
- Called from (representative examples):
  - fmt (formatting command handler in command.c for setting border styles)

## Notes and Other Information
- The function is declared as static, limiting its scope to the command.c compilation unit
- Returns true on successful parsing, false if the input string doesn't match any known line style
- Uses case-insensitive comparison allowing user input like "Single", "DOUBLE", etc.
- The linestyle parameter is only modified if the function returns true, ensuring atomic updates
- Supports exactly two line styles: single and double Unicode borders
- Part of psql's table formatting system that allows users to customize table appearance