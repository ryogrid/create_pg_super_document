# GetCurrentTimeUsec

## Location
[src/backend/utils/adt/datetime.c:387-447](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datetime.c#L387-L447)

## Overview
Gets the current transaction start time ("now()") as a broken-down time structure with fractional seconds and timezone offset, converted according to the session timezone setting.

## Definition
```c
void GetCurrentTimeUsec(struct pg_tm *tm, fsec_t *fsec, int *tzp)
```

## Detailed Description
The `GetCurrentTimeUsec` function provides the most complete interface for obtaining the current transaction start time in PostgreSQL. Unlike `GetCurrentDateTime`, this function includes fractional seconds precision (microseconds) and can optionally provide the timezone offset information.

The function implements an intelligent caching mechanism to optimize performance, since it may be called many times within a single transaction and the "now()" value should remain constant throughout the transaction. The cache key includes both the current timestamp and the current timezone setting, ensuring that timezone changes invalidate the cache appropriately.

The caching strategy assumes that distinct timezone settings never have the same pointer value, which is guaranteed by PostgreSQL's timezone hashtable implementation. If the cache is invalid (different timestamp or timezone), the function performs a fresh conversion using `timestamp2tm` and updates all cached values atomically.

## Parameters / Member Variables
- `tm`: Pointer to a `struct pg_tm` structure that will receive the broken-down time components (output)
- `fsec`: Pointer to an `fsec_t` variable that will receive the fractional seconds (microseconds) (output)  
- `tzp`: Pointer to an integer that will receive the timezone offset in seconds, or NULL if not needed (output)

## Dependencies
- Functions called/Symbols referenced:
  - GetCurrentTransactionStartTimestamp (gets the base timestamp)
  - timestamp2tm (performs the actual timestamp to broken-down time conversion)
  - pg_tm (PostgreSQL's broken-down time structure)
  - fsec_t (fractional seconds type)
  - pg_tz (timezone structure)
  - session_timezone (global session timezone setting)
- Called from (representative examples):
  - GetSQLCurrentTime (SQL CURRENT_TIME function)
  - GetSQLLocalTime (SQL LOCALTIME function)  
  - GetCurrentDateTime (simplified wrapper function)
  - DecodeDateTime (datetime parsing for "now" references)
  - DecodeTimeOnly (time parsing with current date context)

## Notes and Other Information
- Implements transaction-consistent time semantics where "now()" returns the same value throughout a transaction
- Uses an efficient caching mechanism that caches results based on both timestamp and timezone
- The cache invalidation strategy ensures timezone changes are handled correctly
- Provides microsecond precision for fractional seconds, supporting PostgreSQL's high-precision timestamp types
- The timezone offset parameter is optional - callers can pass NULL if they don't need it
- Error handling includes validation that the current timestamp is within the supported range
- Critical for implementing SQL standard time functions that require high precision and timezone awareness
- The caching mechanism significantly improves performance for applications that frequently query current time within transactions
- Part of PostgreSQL's comprehensive date/time infrastructure that ensures consistent temporal semantics