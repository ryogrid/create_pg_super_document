# interval2itm

## Location
[src/backend/utils/adt/timestamp.c:2047-2076](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L2047-L2076)

## Overview
Converts a PostgreSQL Interval data type to a human-readable interval time structure (struct pg_itm), breaking down the interval into years, months, days, hours, minutes, seconds, and microseconds.

## Definition

```c
void
interval2itm(Interval span, struct pg_itm *itm)
```
## Detailed Description
The  function decomposes a PostgreSQL interval into its constituent time components for easier manipulation and display. The conversion process involves:

1. **Month/Year Extraction**: Extracts years and remaining months from the total month count
2. **Day Component**: Directly copies the day component 
3. **Time Decomposition**: Systematically breaks down the microsecond time value into hours, minutes, seconds, and remaining microseconds using division and modulo operations
4. **Component Assignment**: Populates all fields of the pg_itm structure with the calculated values

The function performs no validation or overflow checking since the pg_itm structure fields are designed to accommodate all possible interval values.

## Parameters / Member Variables
- : Input Interval value to decompose containing month, day, and time (microseconds) components
- : Output struct pg_itm to populate with broken-down interval components (tm_year, tm_mon, tm_mday, tm_hour, tm_min, tm_sec, tm_usec)

## Dependencies
- Functions called/Symbols referenced:
  - MONTHS_PER_YEAR (constant for year/month conversion)
  - USECS_PER_HOUR, USECS_PER_MINUTE, USECS_PER_SEC (time conversion constants)
  - TimeOffset (type for intermediate calculations)
- Called from (representative examples):
  - [interval_out](interval_out.md) (interval to string conversion)
  - [interval_to_char](interval_to_char.md) (formatted interval output)
  - [interval_trunc](interval_trunc.md) (interval truncation operations)
  - [interval_part_common](interval_part_common.md) (EXTRACT function for intervals)

## Notes and Other Information
- This is a void function that always succeeds (no error return)
- No overflow checking is performed as pg_itm fields can handle all possible interval values
- The function handles both positive and negative intervals correctly
- Time component extraction uses systematic division to avoid precision loss
- The pg_itm structure uses separate fields for each time component, making it ideal for formatting and extraction operations
- This function is the complement to itm2interval and is essential for interval display and manipulation

## Simplified Source

```c
void
interval2itm(Interval span, struct pg_itm *itm) {
    // Extract years and months from total month count
    itm->tm_year = span.month / MONTHS_PER_YEAR;
    itm->tm_mon = span.month % MONTHS_PER_YEAR;
    itm->tm_mday = span.day;

    // Break down time (microseconds) into components
    TimeOffset time = span.time;

    // Extract hours
    itm->tm_hour = time / USECS_PER_HOUR;
    time %= USECS_PER_HOUR;

    // Extract minutes
    itm->tm_min = time / USECS_PER_MINUTE;
    time %= USECS_PER_MINUTE;

    // Extract seconds
    itm->tm_sec = time / USECS_PER_SEC;
    time %= USECS_PER_SEC;

    // Remaining microseconds
    itm->tm_usec = time;
}
```