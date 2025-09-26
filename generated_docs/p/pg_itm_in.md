# pg_itm_in

## Location
src/include/datatype/timestamp.h: 82 - 91

## Overview
The `pg_itm_in` struct is a specialized data structure for decoding intervals in PostgreSQL, containing only the essential fields needed during interval parsing while avoiding unnecessary memory overhead.

## Definition
```c
struct pg_itm_in
{
    int64   tm_usec;    /* needs to be wide */
    int     tm_mday;
    int     tm_mon;
    int     tm_year;
};
```

## Detailed Description
The `pg_itm_in` structure is designed specifically for interval decoding operations. While PostgreSQL could use the full `struct pg_itm` for this purpose, `pg_itm_in` provides a more efficient alternative by including only the fields that are actually used during interval parsing. This design decision prevents the requirement for `tm_usec` to be 64 bits from propagating to places where it's not necessary, and omitting unused fields serves as an error-prevention measure.

The structure contains only the time components that are relevant during the interval decoding process: microseconds, days, months, and years. The microseconds field is specifically widened to 64 bits to handle large values that may occur during parsing operations.

## Parameters / Member Variables
- `tm_usec`: Microseconds component (uses int64 for wide range support during parsing)
- `tm_mday`: Days component of the interval
- `tm_mon`: Months component of the interval  
- `tm_year`: Years component of the interval

## Dependencies
- Functions called/Symbols referenced:
  - None (this is a data structure)
- Called from (representative examples):
  - `AdjustFractMicroseconds` (src/backend/utils/adt/datetime.c:538)
  - `AdjustFractDays` (src/backend/utils/adt/datetime.c:570)
  - `AdjustFractYears` (src/backend/utils/adt/datetime.c:602)
  - `AdjustMicroseconds` (src/backend/utils/adt/datetime.c:619)
  - `AdjustDays` (src/backend/utils/adt/datetime.c:633)
  - `AdjustMonths` (src/backend/utils/adt/datetime.c:649)
  - `AdjustYears` (src/backend/utils/adt/datetime.c:662)
  - `DecodeTimeForInterval` (src/backend/utils/adt/datetime.c:2702)
  - `ClearPgItmIn` (src/backend/utils/adt/datetime.c:3340)
  - `DecodeInterval` (src/backend/utils/adt/datetime.c:3365)
  - `DecodeISO8601Interval` (src/backend/utils/adt/datetime.c:3830)
  - `interval_in` (src/backend/utils/adt/timestamp.c:909)
  - `itmin2interval` (src/backend/utils/adt/timestamp.c:2115)

## Notes and Other Information
- Specialized purpose: Unlike `pg_itm`, this structure is specifically optimized for interval decoding operations
- Memory efficiency: By omitting unused fields (tm_sec, tm_min, tm_hour), it reduces memory footprint during parsing
- Wide microseconds field: The `tm_usec` field uses int64 to handle potentially large values during interval parsing
- Error prevention: The selective inclusion of fields helps prevent incorrect usage patterns
- Primary use in interval input parsing functions and various adjustment functions for time components
- Part of PostgreSQL's interval parsing infrastructure, particularly for handling textual interval representations