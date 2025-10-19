# get_timezone_offset

## Location
[src/bin/initdb/findtimezone.c:175-189](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/findtimezone.c#L175-L189)

## Overview
Extracts the GMT offset in seconds from a system struct tm, providing a portable way to determine timezone offset across different platforms.

## Definition

```c
static int
get_timezone_offset(struct tm *tm)
```
## Detailed Description
The get_timezone_offset function provides a platform-independent interface for obtaining the GMT offset from a struct tm. It handles the differences in how various operating systems store timezone offset information by using conditional compilation.

The function implements three different approaches based on what's available on the target platform:
1. **HAVE_STRUCT_TM_TM_ZONE**: Uses the tm_gmtoff field directly from the tm structure (common on modern Unix systems)
2. **HAVE_INT_TIMEZONE**: Uses the global TIMEZONE_GLOBAL variable with sign negation (older systems)
3. **Fallback**: Triggers a compilation error if neither method is available, as this should theoretically never happen

The function returns the offset in seconds, where positive values indicate time zones east of GMT and negative values indicate time zones west of GMT.

## Parameters / Member Variables
- `*tm`: Pointer to a struct tm containing time information, typically obtained from system time functions like localtime() or gmtime()
## Dependencies
- Functions called/Symbols referenced:
  - TIMEZONE_GLOBAL (global variable, when HAVE_INT_TIMEZONE is defined)
- Called from (representative examples):
  - Currently no direct callers found in the analyzed codebase

## Notes and Other Information
- Returns timezone offset in seconds from GMT (positive for east, negative for west)
- This is a static function, only accessible within the findtimezone.c file
- Uses conditional compilation to handle platform differences in timezone offset storage
- The tm_gmtoff field is a BSD/GNU extension that provides direct access to the GMT offset
- The TIMEZONE_GLOBAL approach uses an older mechanism and requires sign negation
- The compilation error ensures that unsupported platforms are caught at build time
- This function appears to be utility code that may be used for timezone validation or conversion operations during database initialization

## Simplified Source
```c
static int
get_timezone_offset(struct tm *tm)
{
    // Use platform-specific method to get GMT offset in seconds
#if defined(HAVE_STRUCT_TM_TM_ZONE)
    // Modern systems: tm_gmtoff field directly provides the offset
    return tm->tm_gmtoff;
#elif defined(HAVE_INT_TIMEZONE)
    // Older systems: use global timezone variable (negated)
    return -TIMEZONE_GLOBAL;
#else
    // Should never happen - all supported platforms have one of the above
#error No way to determine TZ? Can this happen?
#endif
}
```