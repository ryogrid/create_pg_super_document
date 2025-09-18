# DecodeInterval

## Location
[src/interfaces/ecpg/pgtypeslib/interval.c:326-679](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/interval.c#L326-L679)

## Overview
Interprets previously parsed fields for general time interval processing, converting tokenized interval string components into PostgreSQL's internal interval representation.

## Definition
```c
int DecodeInterval(char **field, int *ftype, int nf, int range, int *dtype, struct pg_itm_in *itm_in)
```

## Detailed Description
This function processes tokenized interval string components and converts them into PostgreSQL's internal interval time structure. It handles various interval formats including PostgreSQL-style intervals, SQL standard intervals, and supports special interval values like infinity.

The function implements sophisticated sign handling logic depending on the IntervalStyle setting. In SQL_STANDARD mode, a leading negative sign applies to all fields unless other explicit signs are present. It processes fields from right to left to handle units that precede values.

Key capabilities include:
- Support for all time units from microseconds to millennia
- Handling of fractional values and year-month syntax (e.g., "2-6" for 2 years 6 months)
- Processing of time notation (HH:MM:SS) with proper sign handling
- Special handling for "ago" keyword to negate the entire interval
- Support for infinite interval values (DTK_EARLY, DTK_LATE)
- Comprehensive overflow checking and error handling

## Parameters / Member Variables
- `field`: Array of string tokens representing interval components
- `ftype`: Array of field types corresponding to each token (DTK_TIME, DTK_NUMBER, DTK_STRING, etc.)
- `nf`: Number of fields in the arrays
- `range`: Interval typmod constraining which fields are allowed
- `dtype`: Output parameter indicating the result type (DTK_DELTA for normal intervals, or special values for infinity)
- `itm_in`: Output parameter containing the parsed interval components

## Dependencies
- Functions called/Symbols referenced:
  - [ClearPgItmIn](../C/ClearPgItmIn.md) - initializes the interval structure
  - DecodeTimeForInterval - parses time notation (HH:MM:SS)
  - strtoi64, strtoint - string to integer conversion
  - ParseFraction - parses fractional components
  - AdjustYears, AdjustMonths, AdjustDays, AdjustMicroseconds - adjust interval components
  - AdjustFractYears, AdjustFractDays, AdjustFractMicroseconds - adjust fractional components
  - [DecodeUnits](DecodeUnits.md), DecodeSpecial - decode unit names and special keywords
  - [pg_mul_s64_overflow](../p/pg_mul_s64_overflow.md), pg_add_s64_overflow - overflow-safe arithmetic
  - Constants: DTK_DELTA, DTK_TIME, DTK_NUMBER, DTK_STRING, DTK_SPECIAL, USECS_PER_SEC, USECS_PER_MINUTE, USECS_PER_HOUR, USECS_PER_DAY, DAYS_PER_MONTH, MONTHS_PER_YEAR
- Called from:
  - [interval_in](../i/interval_in.md) (src/backend/utils/adt/timestamp.c:933)
  - [PGTYPESinterval_from_asc](../P/PGTYPESinterval_from_asc.md) (src/interfaces/ecpg/pgtypeslib/interval.c:1032)

## Notes and Other Information
- Returns 0 on success, or a DTERR error code for invalid input
- Processes fields from right to left to handle unit specifications that precede their values
- Supports both PostgreSQL traditional and SQL standard sign handling modes
- The "ago" keyword must appear at the end and negates the entire interval
- Special reserved words "infinity" and "-infinity" result in infinite interval values
- Fractional parts are supported for all time units, not just the least significant
- Comprehensive validation prevents field duplication and ensures proper unit-value pairing