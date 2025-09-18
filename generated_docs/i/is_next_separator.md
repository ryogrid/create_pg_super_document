# is_next_separator

## Location
src/backend/utils/adt/formatting.c: 2321 - 2353

## Overview
Determines whether the next format node in a formatting pattern represents a non-digit separator rather than a numeric value.

## Definition
```c
static bool is_next_separator(FormatNode *n)
```

## Detailed Description
This function analyzes a FormatNode to determine if the next format element in a formatting string is a separator (non-digit) character rather than a numeric digit. It's used during parsing of date/time and numeric formatting patterns to distinguish between elements that represent actual data values versus formatting separators. The function handles various node types and special cases, including suffix patterns (like TH/th ordinal suffixes) and end-of-format conditions.

## Parameters / Member Variables
- `n`: Pointer to the current FormatNode in the formatting pattern sequence

## Dependencies
- Functions called/Symbols referenced:
  - FormatNode (structure type)
  - NODE_TYPE_END (constant for end node type)
  - NODE_TYPE_ACTION (constant for action node type) 
  - S_THth (macro to test for TH/th suffix)
  - isdigit (standard library function)
- Called from (representative examples):
  - from_char_parse_int_len

## Notes and Other Information
- This is a static function local to src/backend/utils/adt/formatting.c
- Returns true if the next format element is a separator, false if it's a digit
- Handles special cases like ordinal suffixes (TH, th) which are treated as separators
- End of format string is considered equivalent to a separator
- Used in parsing logic to determine field boundaries in formatted input strings
- Critical for proper parsing of numeric and date/time values from formatted text
- The function advances the pointer to examine the next node (n++)