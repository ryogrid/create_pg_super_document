# pg_gmtime

## Location
[src/timezone/localtime.c:1389-1399](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/localtime.c#L1389-L1399)

## Overview
The `pg_gmtime` function converts a timestamp to Greenwich Mean Time (UTC) representation, returning broken-down time components in a `pg_tm` structure.

## Definition
```c
struct pg_tm *pg_gmtime(const pg_time_t *timep)
```

## Detailed Description
The `pg_gmtime` function is PostgreSQL's equivalent of the standard C library's `gmtime` function. It converts a timestamp to Coordinated Universal Time (UTC/GMT) without applying any timezone adjustments. The function delegates to `gmtsub` with a zero offset parameter, which performs the actual time breakdown calculations. This function is used when UTC time representation is needed, such as for internal timestamp storage, backup manifest timestamps, or when converting between different time representations.

## Parameters / Member Variables
- `timep`: Pointer to a `pg_time_t` value representing the timestamp to convert (typically seconds since Unix epoch)

## Dependencies
- Functions called/Symbols referenced:
  - [gmtsub](../g/gmtsub.md)
  - pg_time_t
- Called from (representative examples):
  - [AddFileToBackupManifest](../A/AddFileToBackupManifest.md)
  - [GetEpochTime](../G/GetEpochTime.md)
  - Various timezone and timestamp handling functions

## Notes and Other Information
This function provides a thread-safe alternative to the standard C library's `gmtime` function within PostgreSQL's timezone infrastructure. Unlike `pg_localtime`, this function does not require a timezone parameter since it always converts to UTC. The function returns a pointer to a static `pg_tm` structure, so the result should be used immediately or copied if persistence is needed. It's commonly used in PostgreSQL for operations that require consistent, timezone-independent time representations.