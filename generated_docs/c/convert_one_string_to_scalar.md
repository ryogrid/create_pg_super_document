# convert_one_string_to_scalar

## Location
[src/backend/utils/adt/selfuncs.c:4607-4657](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L4607-L4657)

## Overview
Converts a single character string to a normalized scalar value between 0 and 1 using a fractional digit representation based on a specified character range.

## Definition

```c
static double
convert_one_string_to_scalar(char *value, int rangelo, int rangehi)
```
## Detailed Description
This function performs the actual mathematical conversion of a string to a scalar value for selectivity estimation. It treats the string as a fractional number where each character represents a digit in a variable base system. The base is determined by the range of characters (rangehi - rangelo + 1).

The conversion algorithm:
1. **Length Limiting**: Processes at most 12 characters to prevent overflow and maintain reasonable precision, since even with maximum base 256, this ensures the denominator stays within safe floating-point bounds (< 256^13 = 2.03e31).

2. **Fractional Conversion**: Each character is converted to its position within the specified range and added to the result as a fractional component with decreasing significance (like decimal places).

3. **Range Clamping**: Characters outside the specified range are clamped to range boundaries (rangelo-1 or rangehi+1) to maintain ordering relationships.

The formula essentially treats the string as: char[0]/base + char[1]/base² + char[2]/base³ + ...

## Parameters / Member Variables
- : The null-terminated string to convert to a scalar value
- : The lowest character value in the expected range
- : The highest character value in the expected range

## Dependencies
- Functions called/Symbols referenced:
  - strlen (standard C library function)
- Called from (representative examples):
  - [convert_string_to_scalar](convert_string_to_scalar.md) (called 3 times for value, lobound, and hibound)

## Notes and Other Information
- Returns 0.0 for empty strings, providing a consistent baseline
- The 12-character limit provides approximately 12 decimal digits of nominal resolution
- Characters outside the specified range are mapped to boundary values to preserve ordering
- The function is static, indicating it's an internal helper for string-to-scalar conversion
- The variable-base system allows for more accurate representations when the actual character range is narrower than the full ASCII range
- Part of PostgreSQL's selectivity estimation system used by the query planner for optimizing queries involving string comparisons