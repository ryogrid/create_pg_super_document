# DecodeTimeForInterval

## Location
[src/backend/utils/adt/datetime.c:2701-2726](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datetime.c#L2701-L2726)

## Overview
DecodeTimeForInterval is an interval-specific wrapper around DecodeTimeCommon that converts parsed time components into a single microsecond value suitable for PostgreSQL interval calculations.

## Definition

```c
static int
DecodeTimeForInterval(char *str, int fmask, int range,
					  int *tmask, struct pg_itm_in *itm_in)
```
## Detailed Description
DecodeTimeForInterval provides specialized processing for interval parsing by converting all time components (hours, minutes, seconds, microseconds) into a unified microsecond representation. This design supports PostgreSQL's internal interval storage format where time components are accumulated as total microseconds. Key features include:

1. **Unified Time Representation**: Converts hours, minutes, and seconds into microseconds and accumulates them into a single tm_usec field, providing a normalized representation for interval arithmetic.

2. **Overflow-Safe Arithmetic**: Uses int64_multiply_add() function calls to perform safe multiplication and addition operations, preventing integer overflow during the conversion process.

3. **Interval-Specific Design**: Unlike DecodeTime which preserves separate fields for timestamp use, this function consolidates time components for interval storage and calculation purposes.

4. **High Precision Support**: Maintains microsecond precision throughout the conversion process, ensuring no loss of temporal resolution.

The function is specifically designed for interval parsing contexts where time components need to be represented as a total microsecond duration.

## Parameters / Member Variables
- `*str`: Input string containing the time to be parsed
- `fmask`: Field mask indicating which date/time fields are already present
- `range`: Range specification for interval parsing (affects field interpretation)
- `*tmask`: Output parameter receiving a mask of successfully parsed time fields
- `*itm_in`: Output parameter receiving the consolidated time as total microseconds
## Dependencies
- Functions called/Symbols referenced:
  - : Core time parsing functionality
  - : Safe 64-bit multiply-and-add operation
  - : Intermediate time structure for parsing results
  - : Interval-specific time input structure
  - , , : Time conversion constants
- Called from (representative examples):
  - : Main interval parsing function (multiple call sites)

## Notes and Other Information
- Returns 0 for successful parsing, or DTERR_FIELD_OVERFLOW for arithmetic overflow
- Designed specifically for interval contexts rather than timestamp parsing
- Uses overflow-safe arithmetic functions to prevent integer overflow during time unit conversions
- Consolidates all time components into a single microsecond value for simplified interval arithmetic
- Static function indicating it's an internal utility within the datetime parsing system
- The accumulated microsecond approach allows for easier interval addition, subtraction, and scaling operations
- Critical for PostgreSQL's interval type implementation where durations are stored as consolidated microsecond values

## Simplified Source

```c
static int
DecodeTimeForInterval(char *str, int fmask, int range,
                      int *tmask, struct pg_itm_in *itm_in)
{
    struct pg_itm itm;
    int dterr;

    // Use common time parsing logic
    dterr = DecodeTimeCommon(str, fmask, range, tmask, &itm);
    if (dterr)
        return dterr;

    // Start with microseconds from parsed time
    itm_in->tm_usec = itm.tm_usec;

    // Convert and accumulate hours, minutes, seconds to microseconds
    // Using safe arithmetic to prevent overflow
    if (!int64_multiply_add(itm.tm_hour, USECS_PER_HOUR, &itm_in->tm_usec) ||
        !int64_multiply_add(itm.tm_min, USECS_PER_MINUTE, &itm_in->tm_usec) ||
        !int64_multiply_add(itm.tm_sec, USECS_PER_SEC, &itm_in->tm_usec))
        return DTERR_FIELD_OVERFLOW;

    return 0;  // Success
}
```