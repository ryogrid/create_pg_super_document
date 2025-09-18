# range_parse

## Location
src/backend/utils/adt/rangetypes.c: 2322 - 2437

## Overview
Static function that parses a textual representation of a range into its constituent components, extracting boundary flags, lower bound string, and upper bound string from the input.

## Definition
```c
static bool range_parse(const char *string, char *flags, char **lbound_str, char **ubound_str, Node *escontext)
```

## Detailed Description
This comprehensive parsing function handles the conversion of textual range literals into their structured components. It supports the full PostgreSQL range syntax including empty ranges, infinite bounds, and proper escaping mechanisms. The function is modeled after record_in from rowtypes.c.

The supported input syntax follows this grammar:
- `<range> := EMPTY | <lb-inc> <string>, <string> <ub-inc>`
- `<lb-inc> := '[' | '('` (inclusive or exclusive lower bound)  
- `<ub-inc> := ']' | ')'` (inclusive or exclusive upper bound)

Key parsing features:
- Handles "EMPTY" keyword for empty ranges
- Supports infinite bounds (empty strings not in quotes)
- Processes quoted strings with escape sequences
- Validates bracket/parenthesis pairing
- Strips whitespace outside of bound values
- Preserves whitespace within bound strings

The function uses error contexts for proper error handling and can return false on parse failures when using ErrorSaveContext.

## Parameters / Member Variables
- `string`: Input string containing the textual range representation to parse
- `flags`: Output parameter receiving the bitmask of range flags (RANGE_EMPTY, RANGE_LB_INC, RANGE_UB_INC, RANGE_LB_INF, RANGE_UB_INF)
- `lbound_str`: Output parameter receiving palloc'd lower bound string (NULL for infinite bounds)
- `ubound_str`: Output parameter receiving palloc'd upper bound string (NULL for infinite bounds)
- `escontext`: Error context for controlled error handling (can be ErrorSaveContext for soft failures)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strncasecmp](../p/pg_strncasecmp.md) (case-insensitive string comparison)
  - `RANGE_EMPTY_LITERAL` (constant for "EMPTY" keyword)
  - `RANGE_EMPTY`, `RANGE_LB_INC`, `RANGE_UB_INC`, `RANGE_LB_INF`, `RANGE_UB_INF` (range flag constants)
  - [range_parse_bound](range_parse_bound.md) (function to parse individual range bounds)
  - `ereturn` (error return macro for controlled error contexts)
- Called from:
  - [range_in](range_in.md) (src/backend/utils/adt/rangetypes.c:107)

## Notes and Other Information
- This is a static function visible only within rangetypes.c
- Returns true on successful parsing, false on failure (when using ErrorSaveContext)
- Handles complex escaping rules for special characters within bounds
- Empty strings represent infinite bounds unless explicitly quoted
- Supports both double-quote and backslash escaping mechanisms
- Part of PostgreSQL's range type input/output infrastructure
- Performs thorough syntax validation with detailed error messages
- Memory allocation for bound strings uses palloc for PostgreSQL memory management