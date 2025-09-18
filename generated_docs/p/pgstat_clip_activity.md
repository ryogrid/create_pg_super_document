# pgstat_clip_activity

## Location
src/backend/utils/activity/backend_status.c: 1164 - 1197

## Overview
Converts a potentially unsafely truncated activity string into a correctly truncated one, ensuring proper multi-byte character handling and NUL termination.

## Definition
```c
char *pgstat_clip_activity(const char *raw_activity)
```

## Detailed Description
This function is designed to safely handle activity strings that may have been truncated unsafely, particularly in the middle of multi-byte characters. The function takes a raw activity string (which may be from PgBackendStatus.st_activity_raw that could be concurrently modified) and returns a properly clipped version that respects character boundaries.

The function performs the following operations:
1. Creates a safe copy of the input string using `pnstrdup` with a maximum length of `pgstat_track_activity_query_size - 1`
2. Determines the actual length of the copied string
3. Uses `pg_mbcliplen` to find the proper clipping point that doesn't break multi-byte characters
4. NUL-terminates the string at the correct position
5. Returns the properly clipped string allocated in the caller's memory context

The implementation leverages the fact that all supported server encodings allow determination of multi-byte character length from the first byte, enabling safe truncation even when the original string was cut in the middle of a multi-byte character.

## Parameters / Member Variables
- `raw_activity`: The input activity string that may be unsafely truncated and potentially concurrently modified

## Dependencies
- Functions called/Symbols referenced:
  - [pnstrdup](pnstrdup.md): Creates a duplicate string with maximum length limit
  - [pg_mbcliplen](pg_mbcliplen.md): Determines safe clipping position for multi-byte strings
  - `pgstat_track_activity_query_size`: Global variable defining the maximum activity string size

- Called from (representative examples):
  - `pgstat_get_backend_current_activity` (src/backend/utils/activity/backend_status.c:935)
  - `PG_STAT_GET_ACTIVITY_COLS` (src/backend/utils/adt/pgstatfuncs.c:392)
  - [pg_stat_get_backend_activity](pg_stat_get_backend_activity.md) (src/backend/utils/adt/pgstatfuncs.c:758)

## Notes and Other Information
- The returned string is allocated in the caller's memory context and may be freed by the caller
- The function is thread-safe and handles concurrent modifications to the input buffer
- Special care is taken to ensure the result is always NUL-terminated
- The function is specifically designed to work with PostgreSQL's statistics activity tracking system
- Multi-byte character safety is ensured only for server encodings (not client encodings like GB18030)
- Located in src/backend/utils/activity/backend_status.c:1164-1197