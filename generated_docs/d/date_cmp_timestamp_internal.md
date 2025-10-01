# date_cmp_timestamp_internal

## Location
[src/backend/utils/adt/date.c:743-759](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L743-L759)

## Overview
Internal function that compares a date value with a timestamp value, returning an integer indicating their relative order.

## Definition
```c
int32 date_cmp_timestamp_internal(DateADT dateVal, Timestamp dt2)
```

## Detailed Description
This is a crosstype comparison function that enables comparison between PostgreSQL's date and timestamp data types. The function converts the date value to a timestamp and then performs the comparison using timestamp comparison logic. It handles overflow conditions that may occur during date-to-timestamp conversion, particularly for dates that are beyond the valid timestamp range.

The function follows PostgreSQL's comparison convention:
- Returns negative value if dateVal < dt2
- Returns 0 if dateVal == dt2  
- Returns positive value if dateVal > dt2

Special handling for overflow cases:
- When the date converts to a value larger than any finite timestamp, it compares appropriately with infinity timestamps
- Uses assertions to ensure that underflow (-1 overflow case) cannot occur

## Parameters / Member Variables
- `dateVal`: A DateADT value representing the date to be compared (days since 2000-01-01)
- `dt2`: A Timestamp value to compare against (microseconds since 2000-01-01)

## Dependencies
- Functions called/Symbols referenced:
  - [date2timestamp_opt_overflow](date2timestamp_opt_overflow.md) (converts date to timestamp with overflow detection)
  - TIMESTAMP_IS_NOEND (macro to check for positive infinity timestamp)
  - [timestamp_cmp_internal](../t/timestamp_cmp_internal.md) (internal timestamp comparison function)
  - DateADT (PostgreSQL's date type)
  - Timestamp (PostgreSQL's timestamp type)
- Called from (representative examples):
  - [date_eq_timestamp](date_eq_timestamp.md) (date equality with timestamp)
  - [date_ne_timestamp](date_ne_timestamp.md) (date inequality with timestamp)
  - [date_lt_timestamp](date_lt_timestamp.md) (date less than timestamp)
  - [date_cmp_timestamp](date_cmp_timestamp.md) (public comparison interface)
  - [timestamp_eq_date](../t/timestamp_eq_date.md) (timestamp equality with date)
  - [cmpDateToTimestamp](../c/cmpDateToTimestamp.md) (JSON path execution)

## Notes and Other Information
- This is an internal function used as the foundation for all date-timestamp comparison operations
- Handles edge cases where date values might be outside the valid timestamp range
- The overflow handling ensures that very large dates are handled correctly when compared with infinity timestamps
- Part of PostgreSQL's crosstype comparison system that enables operations between different temporal data types
- Located in src/backend/utils/adt/date.c:743-759
- The assertion `Assert(overflow == 0)` indicates that negative overflow (underflow) should never occur in practice

## Simplified Source

```c
int32 date_cmp_timestamp_internal(DateADT dateVal, Timestamp dt2) {
    Timestamp dt1;
    int overflow;

    // Convert date to timestamp, checking for overflow
    dt1 = date2timestamp_opt_overflow(dateVal, &overflow);

    if (overflow > 0) {
        // Date exceeds finite timestamp range
        // Compare with infinity timestamp appropriately
        return TIMESTAMP_IS_NOEND(dt2) ? -1 : +1;
    }

    // Normal case: use standard timestamp comparison
    return timestamp_cmp_internal(dt1, dt2);
}
```