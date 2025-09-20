# SetEpochTimestamp

## Location
[src/backend/utils/adt/timestamp.c:2190-2209](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L2190-L2209)

## Overview
Returns a Timestamp value representing the PostgreSQL epoch (January 1, 2000 00:00:00 UTC).

## Definition

```c
struct pg_tm tt,
			   *tm = &tt;
```
## Detailed Description
SetEpochTimestamp is a utility function that creates and returns a Timestamp value corresponding to the PostgreSQL epoch. The PostgreSQL epoch is defined as January 1, 2000 00:00:00 UTC, which serves as the reference point for PostgreSQL's internal timestamp calculations. This function uses GetEpochTime() to obtain the epoch time in a broken-down time structure (pg_tm), then converts it to PostgreSQL's internal Timestamp representation using tm2timestamp().

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [GetEpochTime](../G/GetEpochTime.md): Retrieves the epoch time in broken-down format
  - [tm2timestamp](../t/tm2timestamp.md): Converts broken-down time to PostgreSQL Timestamp format
  - Timestamp: PostgreSQL's internal timestamp data type
  - [pg_tm](../p/pg_tm.md): PostgreSQL's time structure similar to struct tm

- Called from (representative examples):
  - [timestamp_in](../t/timestamp_in.md): Used for parsing timestamp input
  - [timestamptz_in](../t/timestamptz_in.md): Used for parsing timestamptz input  
  - [timestamp_part_common](../t/timestamp_part_common.md): Used in EXTRACT operations
  - [timestamptz_part_common](../t/timestamptz_part_common.md): Used in EXTRACT operations for timestamptz
  - [PGTYPEStimestamp_from_asc](../P/PGTYPEStimestamp_from_asc.md): Used in ECPG library for timestamp parsing

## Notes and Other Information
- This function is primarily used as a reference point for timestamp calculations and conversions
- The function does not perform error checking on the tm2timestamp conversion as noted in the comment
- It's a core utility function used throughout PostgreSQL's timestamp handling subsystem
- The epoch timestamp serves as a baseline for relative timestamp calculations and parsing operations