# pg_tz_acceptable

## Location
[src/timezone/localtime.c:1890-1906](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/localtime.c#L1890-L1906)

## Overview
This function validates whether a timezone is acceptable for PostgreSQL use by detecting and rejecting leap-second-aware timekeeping.

## Definition


## Detailed Description
pg_tz_acceptable performs a critical validation check to determine if a timezone can be safely used with PostgreSQL's date/time arithmetic. The function specifically tests for leap-second-aware timekeeping, which must be rejected because leap seconds would cause havoc with PostgreSQL's date and time calculations.

The validation works by converting a known reference time (GMT midnight, 2000-01-01) using pg_localtime and checking that the seconds field is exactly zero. Any non-zero seconds value indicates the presence of leap-second handling, which is incompatible with PostgreSQL's expectations.

The reference time is calculated as the difference between the PostgreSQL epoch and Unix epoch, multiplied by seconds per day, giving a precise timestamp for January 1, 2000 at midnight GMT.

## Parameters / Member Variables
- : The timezone structure to validate for PostgreSQL compatibility

## Dependencies
- Functions called/Symbols referenced:
  - [pg_tz](pg_tz.md) (timezone structure type)
  - [pg_tm](pg_tm.md) (PostgreSQL time structure)
  - pg_time_t (PostgreSQL time type)
  - [pg_localtime](pg_localtime.md) (timezone conversion function)
  - POSTGRES_EPOCH_JDATE (PostgreSQL epoch constant)
  - UNIX_EPOCH_JDATE (Unix epoch constant)
  - SECS_PER_DAY (seconds per day constant)
- Called from (representative examples):
  - [check_timezone](../c/check_timezone.md) (src/backend/commands/variable.c:349)
  - [check_log_timezone](../c/check_log_timezone.md) (src/backend/commands/variable.c:431)
  - [score_timezone](../s/score_timezone.md) (src/bin/initdb/findtimezone.c:249)
  - [validate_zone](../v/validate_zone.md) (src/bin/initdb/findtimezone.c:1739)
  - [pg_tzenumerate_next](pg_tzenumerate_next.md) (src/timezone/pgtz.c:481)

## Notes and Other Information
- Returns true if the timezone is acceptable (no leap-second handling detected)
- Returns false if leap-second timekeeping is detected or if pg_localtime fails
- Essential for ensuring timezone compatibility with PostgreSQL's date/time arithmetic
- Uses a specific test timestamp (2000-01-01 00:00:00 GMT) to detect leap-second behavior
- Leap-second handling would interfere with PostgreSQL's assumption of consistent time progression
- Located in src/timezone/localtime.c:1890-1906