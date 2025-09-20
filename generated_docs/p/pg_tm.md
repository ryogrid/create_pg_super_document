# pg_tm

## Location
[src/include/pgtime.h:34-49](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/pgtime.h#L34-L49)

## Overview
A data structure representing a broken-down timestamp in PostgreSQL's internal timezone library, similar to the standard C library's tm structure but with PostgreSQL-specific field conventions.

## Definition

```c
struct pg_tm
{
	int			tm_sec;
	int			tm_min;
	int			tm_hour;
	int			tm_mday;
	int			tm_mon;			/* see above */
	int			tm_year;		/* see above */
	int			tm_wday;
	int			tm_yday;
	int			tm_isdst;
	long int	tm_gmtoff;
	const char *tm_zone;
};
```
## Detailed Description
The  structure is PostgreSQL's internal representation of a broken-down timestamp, providing a way to work with date and time components separately. This structure is crucial for PostgreSQL's datetime processing and timezone handling functionality.

**IMPORTANT CONVENTION DIFFERENCE**: PostgreSQL's  structure follows different conventions than the POSIX standard:
- **tm_mon**: PostgreSQL treats this as counting from 1 (January = 1), while POSIX/IANA timezone library counts from 0 (January = 0)
- **tm_year**: PostgreSQL treats this as relative to 1 BC, while POSIX treats it as relative to 1900

This difference requires careful handling when converting between PostgreSQL's datetime functions and the underlying IANA timezone library code.

## Parameters / Member Variables
- `tm_sec`: Seconds (0-59, or 60 for leap seconds)
- `tm_min`: Minutes (0-59)
- `tm_hour`: Hours (0-23)
- `tm_mday`: Day of month (1-31)
- `tm_mon`: Month (1-12 in PostgreSQL convention, unlike POSIX 0-11)
- `tm_year`: Year (relative to 1 BC in PostgreSQL, unlike POSIX relative to 1900)
- `tm_wday`: Day of week (0-6, Sunday = 0)
- `tm_yday`: Day of year (1-366)
- `tm_isdst`: Daylight saving time flag (positive if DST, 0 if not, negative if unknown)
- `tm_gmtoff`: Offset from GMT in seconds
- `*tm_zone`: Timezone abbreviation string

## Dependencies
- Functions called/Symbols referenced:
  - No direct references (this is a data structure)
- Called from (representative examples):
  -  at src/backend/utils/adt/timestamp.c:1901
  -  at src/backend/utils/adt/timestamp.c:1997
  -  at src/timezone/localtime.c:1356
  -  at src/backend/utils/adt/datetime.c:979
  -  at src/backend/utils/adt/datetime.c:4342
  -  at src/timezone/strftime.c:128

## Notes and Other Information
- This structure is extensively used throughout PostgreSQL's date/time processing functions
- Care must be taken when interfacing with the IANA timezone library due to the different field conventions
- The structure includes timezone-specific fields (, ) that extend beyond the basic POSIX tm structure
- Used in timestamp input/output operations, date arithmetic, timezone conversions, and formatting operations
- The structure is defined in 