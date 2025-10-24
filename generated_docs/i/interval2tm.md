# interval2tm

## Location
[src/interfaces/ecpg/pgtypeslib/interval.c:942-971](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/interval.c#L942-L971)

## Overview
interval2tm is a static utility function that converts an interval data type into a standard tm structure for time manipulation and formatting purposes.

## Definition

```c
static int
interval2tm(interval span, struct tm *tm, fsec_t *fsec)
```
## Detailed Description
This function decomposes an interval structure into its constituent time components and stores them in a standard tm structure along with fractional seconds. It handles the conversion of the interval's month and time fields by extracting years and months from the month field, and breaking down the microsecond-based time field into days, hours, minutes, seconds, and fractional seconds.

The function performs straightforward arithmetic operations to extract each time component, using PostgreSQL's standard time conversion constants. The month field is divided to extract full years and remaining months, while the time field (stored in microseconds) is progressively divided to extract days, hours, minutes, seconds, and remaining microseconds.

## Parameters / Member Variables
- `span`: interval structure containing the source interval data with month and time (microseconds) fields
- `*tm`: Pointer to tm structure that will receive the decomposed time components (tm_year, tm_mon, tm_mday, tm_hour, tm_min, tm_sec)
- `*fsec`: Pointer to fsec_t variable that will receive the fractional seconds (microseconds)
## Dependencies
- Functions called/Symbols referenced:
  - fsec_t (fractional seconds type)
  - interval (interval data structure)
  - MONTHS_PER_YEAR (12 - constant for month-to-year conversion)
  - USECS_PER_DAY (microseconds per day constant)
  - USECS_PER_HOUR (microseconds per hour constant)
  - USECS_PER_MINUTE (microseconds per minute constant)
  - USECS_PER_SEC (microseconds per second constant)
- Called from (representative examples):
  - [PGTYPESinterval_to_asc](../P/PGTYPESinterval_to_asc.md) (interval to string conversion function)

## Notes and Other Information
- Located in src/interfaces/ecpg/pgtypeslib/interval.c:942-971
- Part of the ECPG (Embedded C for PostgreSQL) interface library
- Always returns 0 (success), indicating no error conditions are expected
- The function assumes input interval data is valid and well-formed
- Uses progressive subtraction approach to extract time components from the total microseconds
- Designed specifically for interval formatting and display purposes in client applications

## Simplified Source

```c
static int
interval2tm(interval span, struct tm *tm, fsec_t *fsec)
{
    // Extract years and months from the month field
    if (span.month != 0) {
        tm->tm_year = span.month / MONTHS_PER_YEAR;
        tm->tm_mon = span.month % MONTHS_PER_YEAR;
    } else {
        tm->tm_year = 0;
        tm->tm_mon = 0;
    }

    // Break down microseconds into time components
    int64 time = span.time;

    // Extract days, hours, minutes, seconds progressively
    tm->tm_mday = time / USECS_PER_DAY;
    time -= tm->tm_mday * USECS_PER_DAY;

    tm->tm_hour = time / USECS_PER_HOUR;
    time -= tm->tm_hour * USECS_PER_HOUR;

    tm->tm_min = time / USECS_PER_MINUTE;
    time -= tm->tm_min * USECS_PER_MINUTE;

    tm->tm_sec = time / USECS_PER_SEC;
    *fsec = time - (tm->tm_sec * USECS_PER_SEC);

    return 0;
}
```