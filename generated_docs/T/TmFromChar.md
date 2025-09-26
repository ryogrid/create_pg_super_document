# TmFromChar

## Location
[src/backend/utils/adt/formatting.c:400-430](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L400-L430)

## Overview
A structure used to store intermediate parsing results when converting formatted date/time strings to internal timestamp representations in PostgreSQL's formatting system.

## Definition

```c
typedef struct
{
	FromCharDateMode mode;
	int			hh,
				pm,
				mi,
				ss,
				ssss,
				d,				/* stored as 1-7, Sunday = 1, 0 means missing */
				dd,
				ddd,
				mm,
				ms,
				year,
				bc,
				ww,
				w,
				cc,
				j,
				us,
				yysz,			/* is it YY or YYYY ? */
				clock,			/* 12 or 24 hour clock? */
				tzsign,			/* +1, -1, or 0 if no TZH/TZM fields */
				tzh,
				tzm,
				ff;				/* fractional precision */
	bool		has_tz;			/* was there a TZ field? */
	int			gmtoffset;		/* GMT offset of fixed-offset zone abbrev */
	pg_tz	   *tzp;			/* pg_tz for dynamic abbrev */
	char	   *abbrev;			/* dynamic abbrev */
} TmFromChar;
```

## Detailed Description
TmFromChar serves as an intermediate parsing structure for PostgreSQL's date/time string parsing operations, primarily used by functions like `do_to_timestamp` and `DCH_from_char`. When parsing a formatted date/time string, various format elements are extracted and stored in this structure's fields before being converted to the final timestamp representation.

The structure accumulates parsed values from different format elements (year, month, day, hour, minute, second, timezone, etc.) and tracks parsing state including date mode (Gregorian vs ISO week), clock type (12/24 hour), and timezone information. After parsing completes, the accumulated values are validated and converted into PostgreSQL's internal timestamp format.

The structure is designed to handle incomplete date/time specifications - fields default to 0/NULL and missing components are handled appropriately during final timestamp construction.

## Parameters / Member Variables
- `mode`: Date parsing mode (FromCharDateMode) - tracks whether Gregorian or ISO week date elements are being used
- `hh`: Hour value (0-23 or 1-12 depending on clock mode)
- `pm`: AM/PM indicator (0=AM, 1=PM, used with 12-hour clock)
- `mi`: Minutes (0-59)
- `ss`: Seconds (0-59)
- `ssss`: Seconds since midnight (alternative seconds representation)
- `d`: Day of week (1-7, Sunday=1, 0=missing)
- `dd`: Day of month (1-31)
- `ddd`: Day of year (1-366)
- `mm`: Month (1-12)
- `ms`: Milliseconds (0-999)
- `year`: Year value
- `bc`: BC/AD indicator (1=BC, 0=AD)
- `ww`: Week of year
- `w`: Week of month
- `cc`: Century
- `j`: Julian day number
- `us`: Microseconds (0-999999)
- `yysz`: Year size indicator (2=YY format, 4=YYYY format)
- `clock`: Clock format (CLOCK_12_HOUR=1 or CLOCK_24_HOUR=0)
- `tzsign`: Timezone sign (+1, -1, or 0 if no TZH/TZM fields)
- `tzh`: Timezone hours offset
- `tzm`: Timezone minutes offset
- `ff`: Fractional seconds precision
- `has_tz`: Boolean indicating presence of timezone field in input
- `gmtoffset`: GMT offset in seconds for fixed-offset timezone abbreviations
- `tzp`: Pointer to timezone structure for dynamic timezone abbreviations
- `abbrev`: String containing dynamic timezone abbreviation

## Dependencies
- Functions called/Symbols referenced:
  - [FromCharDateMode](../F/FromCharDateMode.md) enum (for mode tracking)
  - [pg_tz](../p/pg_tz.md) structure (for timezone handling)
  - ZERO_tmfc macro (for initialization)
- Called from (representative examples):
  - [DCH_from_char](../D/DCH_from_char.md): Main parsing function that populates TmFromChar
  - [do_to_timestamp](../d/do_to_timestamp.md): Uses TmFromChar for timestamp conversion
  - [from_char_set_mode](../f/from_char_set_mode.md): Validates and sets parsing mode

## Notes and Other Information
- Initialized using the ZERO_tmfc macro which zeroes all fields using memset
- Field values of 0 typically indicate missing or unspecified components
- The structure supports both Gregorian calendar and ISO 8601 week date parsing modes
- Timezone handling supports both fixed offsets (via gmtoffset) and dynamic zones (via tzp/abbrev)
- Year handling includes special logic for 2-digit vs 4-digit years (yysz field)
- After parsing, values are validated and converted to PostgreSQL's internal timestamp representation in struct pg_tm
- The structure is designed to be stack-allocated and temporary - used only during parsing operations