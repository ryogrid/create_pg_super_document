# pg_itm

## Location
[src/include/datatype/timestamp.h:65-81](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/datatype/timestamp.h#L65-L81)

## Overview
The  struct represents a broken-down interval in PostgreSQL, providing a structured way to work with individual time components (microseconds, seconds, minutes, hours, days, months, years) for temporal calculations.

## Definition

```c
struct pg_itm
{
	int			tm_usec;
	int			tm_sec;
	int			tm_min;
	int64		tm_hour;		/* needs to be wide */
	int			tm_mday;
	int			tm_mon;
	int			tm_year;
};
```
## Detailed Description
The  structure is modeled after  used for timestamps but is specifically designed for representing intervals. Unlike timestamps, there's no special interpretation needed for months or years - they're simply zero or non-zero values. This structure allows PostgreSQL to work with interval components in a broken-down format, making it easier to perform arithmetic operations and conversions.

The structure supports negative values in its fields, which is important for representing negative intervals. However, due to the divisions performed during conversion from , only  could potentially reach . This limitation is significant because the code may need to negate values in various code paths.

## Parameters / Member Variables
- `tm_usec`: Microseconds component of the interval (0-999999)
- `tm_sec`: Seconds component of the interval (0-59)
- `tm_min`: Minutes component of the interval (0-59)
- `tm_hour`: Hours component of the interval (uses int64 for wide range support)
- `tm_mday`: Days component of the interval
- `tm_mon`: Months component of the interval
- `tm_year`: Years component of the interval
## Dependencies
- Functions called/Symbols referenced:
  - None (this is a data structure)
- Called from (representative examples):
  -  (src/backend/utils/adt/datetime.c:2591)
  -  (src/backend/utils/adt/datetime.c:2675)
  -  (src/backend/utils/adt/datetime.c:4585)
  -  (src/backend/utils/adt/formatting.c:4333)
  -  (src/backend/utils/adt/timestamp.c:986)
  -  (src/backend/utils/adt/timestamp.c:2047)
  -  (src/backend/utils/adt/timestamp.c:2077)
  -  (src/backend/utils/adt/timestamp.c:4254)
  -  (src/backend/utils/adt/timestamp.c:5025)
  -  (src/backend/utils/adt/timestamp.c:5959)

## Notes and Other Information
- Historical design decision: The structure follows the same pattern as  for consistency
- Wide hour field: The  field uses  to support very large hour values that could result from interval calculations
- Negative value support: All fields can be negative, but special care must be taken with  which could reach 
- Used extensively in interval parsing, formatting, and arithmetic operations
- Critical for PostgreSQL's temporal data type system, particularly for INTERVAL data type operations