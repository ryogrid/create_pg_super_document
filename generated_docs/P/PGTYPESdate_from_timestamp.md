# PGTYPESdate_from_timestamp

## Location
[src/interfaces/ecpg/pgtypeslib/datetime.c:31-46](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/datetime.c#L31-L46)

## Overview
Converts a PostgreSQL timestamp value to a date value by extracting the date portion and discarding the time information.

## Definition
```c
date PGTYPESdate_from_timestamp(timestamp dt)
```

## Detailed Description
PGTYPESdate_from_timestamp performs a conversion from a PostgreSQL timestamp (which includes both date and time information) to a date value (which contains only date information). The function handles both finite and infinite timestamp values. For finite timestamps, it converts from microseconds since the PostgreSQL epoch to days since the epoch by dividing by USECS_PER_DAY. The function returns a date value that represents the same calendar day as the input timestamp, effectively truncating any time-of-day information.

## Parameters / Member Variables
- `dt`: The timestamp value to convert to a date. Can be finite or infinite (TIMESTAMP_NOT_FINITE)

## Dependencies
- Functions called/Symbols referenced:
  - TIMESTAMP_NOT_FINITE (macro for checking infinite timestamps)
  - USECS_PER_DAY (constant for microseconds per day conversion)
  - date (return type)
- Called from (representative examples):
  - [PGTYPEStimestamp_fmt_asc](PGTYPEStimestamp_fmt_asc.md) (timestamp formatting function)
  - [main](../m/main.md) (in test cases)

## Notes and Other Information
- The function handles infinite timestamp values by leaving the date uninitialized (returning 0)
- The conversion is done by integer division of microseconds by USECS_PER_DAY
- Time zone information is not considered in this conversion
- Part of the ECPG pgtypeslib interface for PostgreSQL type conversions
- Located in src/interfaces/ecpg/pgtypeslib/datetime.c:31-46
- The function suppresses compiler warnings by initializing dDate to 0