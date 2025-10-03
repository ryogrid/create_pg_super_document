# date2timestamptz

## Location
[src/backend/utils/adt/date.c:704-719](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L704-L719)

## Overview
A wrapper function that converts a DateADT value to a TimestampTz (timestamp with time zone), throwing an error if the conversion would result in overflow.

## Definition

```c
static TimestampTz
date2timestamptz(DateADT dateVal)
```
## Detailed Description
The  function provides a simplified interface for date-to-timestamptz conversion by calling  with a NULL overflow parameter. This forces the underlying function to throw an error rather than return an overflow indicator when the date value or subsequent timezone adjustment would result in a value outside the valid timestamptz range. The function is declared static, indicating it's used internally within the date.c module for operations that require strict range validation with timezone awareness.

## Parameters / Member Variables
- `dateVal`: The DateADT input value to be converted to timestamp with time zone
## Dependencies
- Functions called/Symbols referenced:
  - [date2timestamptz_opt_overflow](date2timestamptz_opt_overflow.md): Core conversion function with timezone and overflow handling
  - DateADT: PostgreSQL internal date type
- Called from (representative examples):
  - [date_timestamptz](date_timestamptz.md): SQL function for date to timestamptz conversion

## Notes and Other Information
- Static function scope limits usage to date.c module internal operations
- Provides error-throwing behavior for strict timezone-aware type conversion scenarios
- More complex than date2timestamp due to timezone offset considerations
- Serves as the standard interface for date-to-timestamptz conversion when overflow should be treated as an error condition
- Essential for timezone-aware operations where precise error handling is required
- Incorporates session timezone settings to determine appropriate UTC offset

## Simplified Source

```c
static TimestampTz date2timestamptz(DateADT dateVal) {
    // Convert date to timestamptz, throwing error on overflow
    return date2timestamptz_opt_overflow(dateVal, NULL);
}
```