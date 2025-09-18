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
  - oadd (overflow-safe addition function)
  - MINSPERHOUR, SECSPERMIN, SECSPERHOUR, HOURSPERDAY (time constants)
  - ZIC_MAX, PG_INT32_MAX (overflow protection constants)
  - zic_t (timezone time type)
- Called from (representative examples):
  - [getsave](getsave.md) (for parsing timezone offsets)
  - inzsub (for zone definition parsing)
  - getleapdatetime (for leap second time parsing)
  - rulesub (for rule time parsing)

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