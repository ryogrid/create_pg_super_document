# date2timestamp

## Location
[src/backend/utils/adt/date.c:608-623](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L608-L623)

## Overview
A wrapper function that converts a DateADT value to a Timestamp, throwing an error if the conversion would result in overflow.

## Definition

```c
struct pg_tm tt,
			   *tm = &tt;
```
## Detailed Description
The  function provides a simplified interface for date-to-timestamp conversion by calling  with a NULL overflow parameter. This forces the underlying function to throw an error rather than return an overflow indicator when the date value is outside the valid timestamp range. The function is declared static, indicating it's used internally within the date.c module for operations that require strict range validation.

## Parameters / Member Variables
- : The DateADT input value to be converted to timestamp

## Dependencies
- Functions called/Symbols referenced:
  - [date2timestamp_opt_overflow](date2timestamp_opt_overflow.md): Core conversion function with overflow detection
  - DateADT: PostgreSQL internal date type
- Called from (representative examples):
  - [in_range_date_interval](../i/in_range_date_interval.md): Date range checking with intervals
  - [date_pl_interval](date_pl_interval.md): Date plus interval arithmetic
  - [date_mi_interval](date_mi_interval.md): Date minus interval arithmetic  
  - [date_timestamp](date_timestamp.md): SQL function for date to timestamp conversion
  - [datetime_timestamp](datetime_timestamp.md): DateTime to timestamp conversion

## Notes and Other Information
- Static function scope limits usage to date.c module internal operations
- Provides error-throwing behavior for strict type conversion scenarios
- Return type is declared as TimestampTz but actually returns Timestamp (likely a documentation inconsistency)
- Serves as the standard interface for date-to-timestamp conversion when overflow should be treated as an error condition
- Essential for interval arithmetic operations that require timestamp precision

## Simplified Source

```c
static TimestampTz
date2timestamp(DateADT dateVal)
{
    // Delegate to overflow-handling version with NULL overflow parameter
    // This forces error throwing on overflow rather than graceful handling
    return date2timestamp_opt_overflow(dateVal, NULL);
}
```