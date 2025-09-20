# fmt_tm

## Location
[src/backend/utils/adt/formatting.c:469-481](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L469-L481)

## Overview
A custom time structure used by PostgreSQL's formatting functions to support both timestamps and intervals with extended hour range capability.

## Definition

```c
struct fmt_tm
{
	int			tm_sec;
	int			tm_min;
	int64		tm_hour;
	int			tm_mday;
	int			tm_mon;
	int			tm_year;
	int			tm_wday;
	int			tm_yday;
	long int	tm_gmtoff;
};
```
## Detailed Description
The fmt_tm structure is a custom datetime representation used specifically by PostgreSQL's formatting system for datetime-to-character conversion. It's designed as an enhanced version of the standard pg_tm struct with a key difference: the tm_hour field is 64-bit (int64) instead of the standard int, allowing it to handle very large hour values that can occur in interval calculations. This structure omits the tm_isdst and tm_zone fields from the standard tm struct since they are not needed for formatting operations. The structure supports both timestamp and interval data types, making it versatile for various temporal formatting scenarios.

## Parameters / Member Variables
- `tm_sec`: Seconds (0-60, allowing for leap seconds)
- `tm_min`: Minutes (0-59)
- `tm_hour`: Hours as a 64-bit integer, allowing for very large values when representing intervals
- `tm_mday`: Day of the month (1-31)
- `tm_mon`: Month (0-11, where 0 = January)
- `tm_year`: Year (full year, not offset from 1900)
- `tm_wday`: Day of the week (0-6, where 0 = Sunday)
- `tm_yday`: Day of the year (1-366)
- `tm_gmtoff`: GMT offset in seconds for timezone-aware timestamps

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is a simple data structure)
- Called from (representative examples):
  - [TmToChar](../T/TmToChar.md)
  - [DCH_to_char](../D/DCH_to_char.md)
  - [timestamp_to_char](../t/timestamp_to_char.md)
  - [timestamptz_to_char](../t/timestamptz_to_char.md)
  - [interval_to_char](../i/interval_to_char.md)

## Notes and Other Information
This structure is specifically designed for PostgreSQL's datetime formatting system in src/backend/utils/adt/formatting.c. The most notable feature is the 64-bit tm_hour field, which allows the structure to represent intervals with hour values that exceed the range of a standard 32-bit integer. This is particularly important for interval arithmetic where the hours component can become very large. The structure intentionally omits timezone-related fields (tm_isdst, tm_zone) that are present in standard tm structures since they are not required for formatting operations.