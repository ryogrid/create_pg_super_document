# parse_int

## Location
[src/backend/utils/misc/guc.c:2873-2962](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L2873-L2962)

## Overview
Parses a string value into an integer, supporting various number formats and optional unit suffixes for PostgreSQL configuration parameters.

## Definition
```c
bool parse_int(const char *value, int *result, int flags, const char **hintmsg)
```

## Detailed Description
This function provides comprehensive integer parsing for PostgreSQL's configuration system. It accepts multiple numeric formats including decimal, octal, hexadecimal, and even floating-point numbers (which are rounded to integers). The function also handles unit suffixes when specified by the flags parameter, converting values like "8kB" or "5min" to their base integer representation.

The parsing process is robust and handles edge cases including overflow detection, NaN rejection, and whitespace tolerance. When unit conversion is involved, the function uses convert_to_base_unit() to handle the transformation from user-friendly units to internal base units.

For error cases, the function can provide helpful hint messages to guide users toward correct syntax, particularly for unit specifications.

## Parameters / Member Variables
- `value`: The string to parse as an integer value
- `result`: Output parameter - pointer to store the parsed integer result (can be NULL)
- `flags`: GUC flags indicating whether units are allowed and what type
- `hintmsg`: Output parameter - pointer to store helpful error message (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - strtol (standard library function for integer parsing)
  - strtod (standard library function for floating-point parsing)
  - isnan (mathematical function to check for NaN)
  - rint (mathematical function for rounding)
  - isspace (character classification function)
  - [convert_to_base_unit](../c/convert_to_base_unit.md) (unit conversion function)
  - GUC_UNIT, GUC_UNIT_MEMORY (unit flag constants)
  - memory_units_hint, time_units_hint (hint message constants)
  - gettext_noop (internationalization macro)
- Called from (representative examples):
  - [parse_one_reloption](parse_one_reloption.md)
  - [ExecVacuum](../E/ExecVacuum.md)
  - [parse_and_validate_value](parse_and_validate_value.md)
  - EmitWarningsOnPlaceholders

## Notes and Other Information
- Supports decimal, octal (0-prefix), and hexadecimal (0x-prefix) integer formats
- Accepts floating-point input that gets rounded to integer values
- Handles integer overflow by checking against INT_MAX and INT_MIN
- Provides specific hint messages for memory units vs. time units
- Uses errno to detect parsing errors and range overflow
- Allows whitespace between the numeric value and unit suffix
- Returns false for invalid input while optionally setting helpful error hints