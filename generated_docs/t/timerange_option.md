# timerange_option

## Location
[src/timezone/zic.c:602-643](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L602-L643)

## Overview
Parses and sets the time range for timezone compilation output in the PostgreSQL timezone utility (zic). This function validates and configures the time boundaries for generated timezone data files.

## Definition


## Detailed Description
The timerange_option function processes a time range specification string to set the lo_time and hi_time global variables that control the time range of the output timezone data. It supports two formats:
1. Single timestamp:  - sets the lower bound, upper bound remains at max_time
2. Range format:  - sets both lower and upper bounds

The function performs comprehensive validation including:
- Parsing timestamp values using strtoimax for 64-bit integer conversion
- Checking for overflow conditions and invalid formats
- Ensuring the specified range is within system-defined min_time and max_time limits
- Validating that the lower bound doesn't exceed the upper bound

## Parameters / Member Variables
- : Input string specifying the time range in format  or  where timestamps are Unix epoch values

## Dependencies
- Functions called/Symbols referenced:
  - strtoimax (standard library function for string to integer conversion)
  - PG_INT64_MAX (PostgreSQL macro for 64-bit integer maximum)
  - PG_INT64_MIN (PostgreSQL macro for 64-bit integer minimum)
- Called from (representative examples):
  - [main](../m/main.md) (in src/timezone/zic.c:777)

## Notes and Other Information
- Returns true on successful parsing and validation, false on any error
- Modifies global variables lo_time and hi_time to constrain timezone data generation
- Uses errno checking to detect integer overflow during parsing
- Handles edge cases like maximum integer values and range validation
- Part of the PostgreSQL timezone compiler (zic) utility for creating timezone data files