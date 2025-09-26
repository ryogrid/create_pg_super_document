# parse_real

## Location
[src/backend/utils/misc/guc.c:2963-3024](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L2963-L3024)

## Overview
Parses a string value into a floating-point number, supporting optional unit suffixes for PostgreSQL configuration parameters.

## Definition
```c
bool parse_real(const char *value, double *result, int flags, const char **hintmsg)
```

## Detailed Description
This function provides floating-point number parsing for PostgreSQL's configuration system. It uses the standard strtod() function to parse floating-point values in the usual formats (decimal, scientific notation, etc.). Like its integer counterpart parse_int(), it supports optional unit suffixes when specified by the flags parameter.

The function handles unit conversion by delegating to convert_to_base_unit(), which transforms user-friendly units like "1.5GB" or "2.5min" into their base unit equivalents. It includes proper error handling for malformed input, range errors, and invalid unit specifications.

The function rejects NaN values but allows infinities (which will be caught by subsequent range checks in the calling code). It's more lenient than parse_int() in that it doesn't need to perform rounding operations.

## Parameters / Member Variables
- `value`: The string to parse as a floating-point value
- `result`: Output parameter - pointer to store the parsed double result (can be NULL)
- `flags`: GUC flags indicating whether units are allowed and what type
- `hintmsg`: Output parameter - pointer to store helpful error message (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - strtod (standard library function for floating-point parsing)
  - isnan (mathematical function to check for NaN)
  - isspace (character classification function)
  - [convert_to_base_unit](../c/convert_to_base_unit.md) (unit conversion function)
  - GUC_UNIT, GUC_UNIT_MEMORY (unit flag constants)
  - memory_units_hint, time_units_hint (hint message constants)
- Called from (representative examples):
  - [parse_one_reloption](parse_one_reloption.md)
  - [parse_and_validate_value](parse_and_validate_value.md)
  - EmitWarningsOnPlaceholders

## Notes and Other Information
- Accepts standard floating-point formats including scientific notation (e.g., "1.5e3")
- Handles unit suffixes for both memory and time-based parameters
- Provides specific hint messages for memory units vs. time units on error
- Uses errno to detect parsing errors and range overflow
- Allows whitespace between the numeric value and unit suffix
- Rejects NaN values but allows infinite values (handled by caller)
- Simpler than parse_int() since no rounding or integer overflow checks are needed
- Returns false for invalid input while optionally setting helpful error hints