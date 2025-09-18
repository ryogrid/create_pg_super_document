# printPsetInfo

## Location
[src/bin/psql/command.c:4886-5085](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L4886-L5085)

## Overview
A comprehensive display function in psql that prints the current state of formatting parameters, providing users with detailed information about their output settings.

## Definition
```c
static bool printPsetInfo(const char *param, printQueryOpt *popt)
```

## Detailed Description
The `printPsetInfo` function serves as the primary interface for displaying psql's formatting parameter states to users. It takes a parameter name and the current print options structure, then outputs a human-readable description of the specified setting's current value. This function is central to psql's \\pset command when used without values (to show current settings). It handles a comprehensive range of formatting options including border styles, field separators, output formats, pager settings, Unicode line styles, and various display toggles. The function uses internationalized messages and provides detailed feedback about each formatting parameter's current state.

## Parameters / Member Variables
- `param`: String specifying which formatting parameter to display (e.g., "border", "format", "fieldsep")
- `popt`: Pointer to printQueryOpt structure containing the current formatting settings to be displayed

## Dependencies
- Functions called/Symbols referenced:
  - [_align2string](../a/_align2string.md) (converts format enum to string representation)
  - [_unicode_linestyle2string](../u/_unicode_linestyle2string.md) (converts Unicode line style enum to string)
  - [get_line_style](../g/get_line_style.md) (retrieves line style information)
  - ngettext (internationalization function for plural forms)
  - Various enum constants (PRINT_XHEADER_*, etc.)
- Called from (representative examples):
  - fmt (formatting command handler in command.c)

## Notes and Other Information
- The function is declared as static, limiting its scope to the command.c compilation unit
- Returns true on successful parameter display, false if the parameter name is unknown
- Supports all major psql formatting parameters: border, columns, expanded display, field separators, footer, format, line style, null display, numeric locale, pager settings, record separators, table attributes, title, tuples-only mode, and Unicode line styles
- Uses internationalized messages through gettext macros (_() and ngettext()) for localization support
- Handles special cases like zero-byte separators and different expanded header width types
- Provides detailed, user-friendly descriptions of complex formatting states
- Part of psql's comprehensive formatting system that allows users to inspect their current output settings
- Error handling includes logging unknown parameter names with pg_log_error