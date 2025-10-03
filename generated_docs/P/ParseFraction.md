# ParseFraction

## Location
[src/backend/utils/adt/datetime.c:680-708](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datetime.c#L680-L708)

## Overview
Parses the fractional part of a number (decimal point and optional digits) and converts it to a double value, handling edge cases like standalone decimal points.

## Definition

```c
static int
ParseFraction(char *cp, double *frac)
```
## Detailed Description
ParseFraction is a utility function responsible for parsing fractional seconds in time/date strings. It expects the input string to start with a decimal point followed by optional digits. The function uses strtod() for the actual conversion but includes special handling for the edge case where only a decimal point is provided without any digits, which some versions of strtod() would reject with EINVAL.

The function performs strict validation, ensuring that the entire input string after the decimal point consists only of valid digits, and returns appropriate error codes for malformed input.

## Parameters / Member Variables
- `*cp`: Pointer to the character string starting with decimal point, representing the fractional part to parse
- `*frac`: Output parameter that receives the parsed fractional value as a double
## Dependencies
- Functions called/Symbols referenced:
  - DTERR_BAD_FORMAT (error constant)
  - strtod (standard library function)
  - Assert (assertion macro)
- Called from (representative examples):
  - [ParseFractionalSecond](ParseFractionalSecond.md)
  - [DecodeDateTime](../D/DecodeDateTime.md)  
  - [DecodeTimeOnly](../D/DecodeTimeOnly.md)
  - [DecodeInterval](../D/DecodeInterval.md)

## Notes and Other Information
- Returns 0 on success, DTERR_BAD_FORMAT on parsing errors
- Handles the special case of standalone decimal point by setting fraction to 0
- Uses errno checking to detect strtod() failures
- Requires input string to start with decimal point (enforced by assertion)
- Validates that entire input string after decimal point is consumed during parsing

## Simplified Source

```c
static int ParseFraction(char *cp, double *frac) {
    // Input must start with decimal point
    Assert(*cp == '.');

    // Handle standalone decimal point case
    if (cp[1] == '\0') {
        *frac = 0;
        return 0;
    }

    // Parse fractional digits using standard library
    errno = 0;
    *frac = strtod(cp, &cp);

    // Validate complete consumption and no errors
    if (*cp != '\0' || errno != 0) {
        return DTERR_BAD_FORMAT;
    }

    return 0;
}
```