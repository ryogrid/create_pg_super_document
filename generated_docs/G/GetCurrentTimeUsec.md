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
  - [GetCurrentTransactionStartTimestamp](GetCurrentTransactionStartTimestamp.md) (gets the base timestamp)
  - [timestamp2tm](../t/timestamp2tm.md) (performs the actual timestamp to broken-down time conversion)
  - [pg_tm](../p/pg_tm.md) (PostgreSQL's broken-down time structure)
  - fsec_t (fractional seconds type)
  - [pg_tz](../p/pg_tz.md) (timezone structure)
  - session_timezone (global session timezone setting)
- Called from (representative examples):
  - [GetSQLCurrentTime](GetSQLCurrentTime.md) (SQL CURRENT_TIME function)
  - [GetSQLLocalTime](GetSQLLocalTime.md) (SQL LOCALTIME function)  
  - [GetCurrentDateTime](GetCurrentDateTime.md) (simplified wrapper function)
  - [DecodeDateTime](../D/DecodeDateTime.md) (datetime parsing for "now" references)
  - [DecodeTimeOnly](../D/DecodeTimeOnly.md) (time parsing with current date context)

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

## Simplified Source

```c
void
GetCurrentTimeUsec(struct pg_tm *tm, fsec_t *fsec, int *tzp)
{
    TimestampTz cur_ts = GetCurrentTransactionStartTimestamp();

    // Static cache to avoid repeated conversions within a transaction
    static TimestampTz cache_ts = 0;
    static pg_tz *cache_timezone = NULL;
    static struct pg_tm cache_tm;
    static fsec_t cache_fsec;
    static int cache_tz;

    // Check if we need to refresh the cache
    if (cur_ts != cache_ts || session_timezone != cache_timezone) {
        // Mark cache invalid during update to handle potential errors
        cache_timezone = NULL;

        // Convert current timestamp to broken-down time
        if (timestamp2tm(cur_ts, &cache_tz, &cache_tm, &cache_fsec,
                        NULL, session_timezone) != 0) {
            ereport(ERROR,
                    (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                     errmsg("timestamp out of range")));
        }

        // Mark cache as valid
        cache_ts = cur_ts;
        cache_timezone = session_timezone;
    }

    // Return cached results
    *tm = cache_tm;
    *fsec = cache_fsec;
    if (tzp != NULL) {
        *tzp = cache_tz;
    }
}
```