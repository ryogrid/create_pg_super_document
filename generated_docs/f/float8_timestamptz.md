# float8_timestamptz

## Location
[src/backend/utils/adt/timestamp.c:735-784](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L735-L784)

## Overview
PostgreSQL SQL function that converts a Unix epoch timestamp (as double precision seconds) to a PostgreSQL timestamptz value.

## Definition

```c
struct pg_tm tt,
			   *tm = &tt;
```
## Detailed Description
The  function is a PostgreSQL SQL-callable function that implements the  SQL function. It converts a Unix epoch timestamp (seconds since January 1, 1970 UTC) to PostgreSQL's internal timestamptz representation.

The function handles several special cases:
- NaN input values are rejected with an error
- Infinite input values are converted to PostgreSQL's special timestamp values (TIMESTAMP_NOBEGIN for negative infinity, TIMESTAMP_NOEND for positive infinity)
- Range validation to ensure the timestamp falls within PostgreSQL's supported timestamp range
- Epoch conversion from Unix epoch to PostgreSQL epoch (January 1, 2000)
- Precision conversion from seconds to microseconds

The function performs multiple validation checks to ensure the resulting timestamp is valid and within PostgreSQL's supported range.

## Parameters / Member Variables
- Function takes 1 PostgreSQL function argument:
  -  (float8): Unix epoch timestamp as seconds since January 1, 1970 UTC

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8 (macro)
  - isnan
  - isinf
  - TIMESTAMP_NOBEGIN (macro)
  - TIMESTAMP_NOEND (macro)
  - rint
  - IS_VALID_TIMESTAMP
  - PG_RETURN_TIMESTAMP (macro)
  - Constants: SECS_PER_DAY, DATETIME_MIN_JULIAN, UNIX_EPOCH_JDATE, TIMESTAMP_END_JULIAN, POSTGRES_EPOCH_JDATE, USECS_PER_SEC
- Called from:
  - SQL queries (via function call mechanism)

## Notes and Other Information
- This is a PostgreSQL built-in SQL function accessible from SQL statements as to_timestamp()
- Uses the standard PostgreSQL function calling convention with PG_FUNCTION_ARGS
- Converts from Unix epoch (1970-01-01) to PostgreSQL epoch (2000-01-01)
- Handles floating-point precision and converts to microsecond precision internally
- Supports special values for infinite timestamps using PostgreSQL's special timestamp representations
- Performs comprehensive input validation including NaN detection and range checking
- Can be called from SQL as: SELECT to_timestamp(1672531200.5); -- converts Unix timestamp to timestamptz
- Throws ERRCODE_DATETIME_VALUE_OUT_OF_RANGE errors for invalid inputs (NaN, out of range values)
- The result is returned as a timestamptz (timestamp with timezone) type