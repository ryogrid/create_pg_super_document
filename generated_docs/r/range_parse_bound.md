# range_parse_bound

## Location
src/backend/utils/adt/rangetypes.c: 2438 - 2506

## Overview
Static helper function that parses and de-quotes a single bound string from a range literal, handling escape sequences and determining if the bound represents an infinite value.

## Definition
```c
static const char *range_parse_bound(const char *string, const char *ptr, char **bound_str, bool *infinite, Node *escontext)
```

## Detailed Description
This function is a crucial helper for range_parse that handles the detailed parsing of individual range bounds. It scans from the current position until it encounters a delimiter (comma, right parenthesis, or right bracket), processing the bound string with proper quote handling and escape sequence processing.

The function handles several parsing scenarios:
1. **Infinite bounds**: Empty input (immediate delimiter) results in NULL bound_str and infinite=true
2. **Quoted strings**: Strings enclosed in double quotes, supporting quote escaping via doubling ("") 
3. **Unquoted strings**: Regular strings with backslash escaping for special characters
4. **Escape sequences**: Backslash escapes the next character, double quotes can be escaped within quoted sections

The parsing continues until reaching a range delimiter (comma for bound separation, parenthesis/bracket for range termination) unless currently inside a quoted section.

## Parameters / Member Variables
- `string`: The complete input string (used only for error reporting context)
- `ptr`: Current parsing position within the string
- `bound_str`: Output parameter receiving palloc'd bound string (NULL for infinite bounds)
- `infinite`: Output parameter set to true if bound represents infinity, false otherwise
- `escontext`: Error context for controlled error handling

## Dependencies
- Functions called/Symbols referenced:
  - `StringInfoData` (string buffer structure)
  - `initStringInfo` (initialize string buffer)
  - `appendStringInfoChar` (append character to string buffer)
  - `ereturn` (error return macro for controlled error contexts)
- Called from:
  - `range_parse` (src/backend/utils/adt/rangetypes.c:2373) - for lower bound parsing
  - `range_parse` (src/backend/utils/adt/rangetypes.c:2388) - for upper bound parsing

## Notes and Other Information
- This is a static function visible only within rangetypes.c
- Returns updated parsing position pointer, or NULL on error (with ErrorSaveContext)
- Handles complex quoting rules: quotes can be escaped by doubling or backslash
- Empty bounds (no characters before delimiter) are treated as infinite
- Uses StringInfo for efficient string building during parsing
- Part of PostgreSQL's range literal parsing infrastructure
- Supports PostgreSQL's standard escape mechanisms for special characters
- Memory allocation for bound strings uses palloc for PostgreSQL memory management