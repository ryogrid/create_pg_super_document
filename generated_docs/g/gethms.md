# gethms

## Location
[src/timezone/zic.c:1365-1442](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L1365-L1442)

## Overview
Converts time specification strings in various formats (h, -h, hh:mm, -hh:mm, hh:mm:ss, -hh:mm:ss) into a number of seconds, with comprehensive validation and rounding support.

## Definition
```c
static zic_t gethms(char const *string, char const *errstring)
```

## Detailed Description
The `gethms` function is a robust time parser that converts human-readable time specifications into seconds. It supports multiple time formats commonly used in timezone definitions:

1. **Format Support**: Handles single hour values (h), hour:minute pairs (hh:mm), and full hour:minute:second specifications (hh:mm:ss), all with optional negative signs for time offsets.

2. **Fractional Seconds**: Supports fractional seconds with decimal notation (hh:mm:ss.t) with proper rounding to the nearest even second.

3. **Validation**: Performs comprehensive validation including:
   - Format validation using sscanf with pattern matching
   - Range checking for hours, minutes, and seconds
   - Overflow detection for large hour values
   - Delimiter validation (ensuring proper : and . separators)

4. **Error Handling**: Reports parsing errors using the provided error string and returns 0 on failure.

5. **Compatibility Warnings**: Issues warnings for features not supported by older versions of zic (fractional seconds and values over 24 hours).

The function uses sophisticated sscanf pattern matching to handle all valid formats in a single parse operation, making it both efficient and maintainable.

## Parameters / Member Variables
- `string`: The time specification string to parse (NULL or empty string returns 0)
- `errstring`: Error message to display if parsing fails

## Dependencies
- Functions called/Symbols referenced:
  - sscanf (standard C parsing function)
  - [warning](../w/warning.md) (warning message function) 
  - [error](../e/error.md) (error reporting function)
  - [oadd](../o/oadd.md) (overflow-safe addition function)
  - MINSPERHOUR, SECSPERMIN, SECSPERHOUR, HOURSPERDAY (time constants)
  - ZIC_MAX, PG_INT32_MAX (overflow protection constants)
  - zic_t (timezone time type)
- Called from (representative examples):
  - [getsave](getsave.md) (for parsing timezone offsets)
  - [inzsub](../i/inzsub.md) (for zone definition parsing)
  - [getleapdatetime](getleapdatetime.md) (for leap second time parsing)
  - [rulesub](../r/rulesub.md) (for rule time parsing)

## Notes and Other Information
- This is a static function with internal linkage in src/timezone/zic.c
- Uses PostgreSQL-specific portability considerations, notably using int instead of zic_t for sscanf compatibility
- Implements "round to even" logic for fractional seconds to ensure consistent behavior
- Includes overflow protection for 32-bit integer systems
- Supports negative time offsets commonly used in timezone specifications
- Issues compatibility warnings for features not supported in older zic versions (pre-2007 and pre-2018)
- Returns 0 for NULL or empty input strings, treating them as neutral time offsets
- The complex sscanf pattern handles up to 9 different parsing scenarios in a single call
- Fractional seconds are rounded to the nearest second using banker's rounding (round to even)

## Simplified Source

```c
static zic_t gethms(char const *string, char const *errstring) {
    int hh, sign, mm = 0, ss = 0;
    char hhx, mmx, ssx, xr = '0', xs;
    int tenths = 0;
    bool ok = true;

    // Handle null/empty string
    if (string == NULL || *string == '\0')
        return 0;

    // Parse sign
    if (*string == '-') {
        sign = -1;
        ++string;
    } else {
        sign = 1;
    }

    // Parse time components using complex sscanf pattern
    switch (sscanf(string, "%d%c%d%c%d%c%1d%*[0]%c%*[0123456789]%c",
                   &hh, &hhx, &mm, &mmx, &ss, &ssx, &tenths, &xr, &xs)) {
        case 8: ok = '0' <= xr && xr <= '9'; /* fallthrough */
        case 7: ok &= ssx == '.'; /* fallthrough */
        case 5: ok &= mmx == ':'; /* fallthrough */
        case 3: ok &= hhx == ':'; /* fallthrough */
        case 1: break;
        default: ok = false; break;
    }

    // Validate parsing result and ranges
    if (!ok || hh < 0 || mm < 0 || mm >= MINSPERHOUR ||
        ss < 0 || ss > SECSPERMIN) {
        error("%s", errstring);
        return 0;
    }

    // Check for hour overflow on 64-bit systems
#if INT_MAX > PG_INT32_MAX
    if (ZIC_MAX / SECSPERHOUR < hh) {
        error(_("time overflow"));
        return 0;
    }
#endif

    // Round fractional seconds to even
    ss += 5 + ((ss ^ 1) & (xr == '0')) <= tenths;

    // Convert to total seconds with overflow protection
    return oadd(sign * (zic_t) hh * SECSPERHOUR,
                sign * (mm * SECSPERMIN + ss));
}
```