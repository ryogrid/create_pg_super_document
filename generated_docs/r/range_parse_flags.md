# range_parse_flags

## Location
[src/backend/utils/adt/rangetypes.c:2247-2321](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L2247-L2321)

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
  - [errcode](../e/errcode.md) (error code specification)
  - [errmsg](../e/errmsg.md) (error message specification)
  - [errhint](../e/errhint.md) (error hint specification)
- Called from:
  - [range_constructor3](range_constructor3.md) (src/backend/utils/adt/rangetypes.c:424)

## Notes and Other Information
- This is a static function, meaning it's only visible within the rangetypes.c file
- The function enforces strict validation - the input must be exactly two characters
- Valid flag combinations are: "[]", "[)", "(]", "()"
- Returns a char bitmask where bits represent boundary inclusivity
- Part of PostgreSQL's range type parsing infrastructure
- Used during range construction to interpret textual range representations
- Provides detailed error messages with hints for invalid input

## Simplified Source

```c
static char
range_parse_flags(const char *flags_str)
{
    char flags = 0;

    // Validate input is exactly 2 characters
    if (flags_str[0] == '\0' || flags_str[1] == '\0' || flags_str[2] != '\0') {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                       errmsg("invalid range bound flags"),
                       errhint("Valid values are \"[]\", \"[)\", \"(]\", and \"()\"")));
    }

    // Parse first character (lower bound)
    switch (flags_str[0]) {
        case '[':
            flags |= RANGE_LB_INC;  // Lower bound inclusive
            break;
        case '(':
            break;  // Lower bound exclusive (no flag)
        default:
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                           errmsg("invalid range bound flags"),
                           errhint("Valid values are \"[]\", \"[)\", \"(]\", and \"()\"")));
    }

    // Parse second character (upper bound)
    switch (flags_str[1]) {
        case ']':
            flags |= RANGE_UB_INC;  // Upper bound inclusive
            break;
        case ')':
            break;  // Upper bound exclusive (no flag)
        default:
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                           errmsg("invalid range bound flags"),
                           errhint("Valid values are \"[]\", \"[)\", \"(]\", and \"()\"")));
    }

    return flags;
}
```