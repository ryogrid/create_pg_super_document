# range_parse_flags

## Location
src/backend/utils/adt/rangetypes.c: 2247 - 2321

## Overview
Static utility function that parses a string representation of range boundary flags and converts it into a character bitmask representing the inclusivity/exclusivity of range bounds.

## Definition
```c
static char range_parse_flags(const char *flags_str)
```

## Detailed Description
This function validates and parses a two-character string that represents the boundary flags for a PostgreSQL range type. The string must be exactly two characters long and represent one of four valid range boundary combinations: "[]", "[)", "(]", or "()". The function converts these string representations into a bitmask using RANGE_LB_INC and RANGE_UB_INC flags.

The first character indicates the lower bound inclusivity:
- '[' means the lower bound is inclusive (RANGE_LB_INC flag set)
- '(' means the lower bound is exclusive (no flag set)

The second character indicates the upper bound inclusivity:
- ']' means the upper bound is inclusive (RANGE_UB_INC flag set)
- ')' means the upper bound is exclusive (no flag set)

The function performs strict validation and throws syntax errors for any invalid input format.

## Parameters / Member Variables
- `flags_str`: A const char pointer to a string that should contain exactly two characters representing range boundary flags

## Dependencies
- Functions called/Symbols referenced:
  - `RANGE_LB_INC` (constant flag for lower bound inclusive)
  - `RANGE_UB_INC` (constant flag for upper bound inclusive)
  - `ereport` (error reporting function)
  - `errcode` (error code specification)
  - `errmsg` (error message specification)
  - `errhint` (error hint specification)
- Called from:
  - `range_constructor3` (src/backend/utils/adt/rangetypes.c:424)

## Notes and Other Information
- This is a static function, meaning it's only visible within the rangetypes.c file
- The function enforces strict validation - the input must be exactly two characters
- Valid flag combinations are: "[]", "[)", "(]", "()"
- Returns a char bitmask where bits represent boundary inclusivity
- Part of PostgreSQL's range type parsing infrastructure
- Used during range construction to interpret textual range representations
- Provides detailed error messages with hints for invalid input