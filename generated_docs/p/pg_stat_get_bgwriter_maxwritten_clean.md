# pg_stat_get_bgwriter_maxwritten_clean

## Location
[src/backend/utils/adt/pgstatfuncs.c:1225-1230](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pgstatfuncs.c#L1225-L1230)

## Overview
Returns the number of times the background writer stopped cleaning scans early due to writing too many buffers since server startup.

## Definition
```c
Datum pg_stat_get_bgwriter_maxwritten_clean(PG_FUNCTION_ARGS)
```

## Detailed Description
This SQL-callable function provides access to the background writer's throttling statistics. It retrieves the cumulative count of times that the background writer had to stop its cleaning scan operations early because it reached the maximum number of buffers allowed to be written in a single cleaning round. This mechanism prevents the background writer from consuming too much I/O bandwidth and interfering with normal database operations. The bgwriter_lru_maxpages configuration parameter controls this limit. A high value for this statistic may indicate that the background writer is being overly constrained and might benefit from tuning.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_fetch_stat_bgwriter](pgstat_fetch_stat_bgwriter.md)
  - PG_RETURN_INT64
- Called from (representative examples):
  - SQL queries via the PostgreSQL function call interface
  - System monitoring and statistics collection queries

## Notes and Other Information
- Returns a 64-bit integer value to accommodate large counts over long server uptimes
- Part of PostgreSQL's statistics collection system for monitoring background writer performance
- Indicates when the background writer is being throttled by the bgwriter_lru_maxpages setting
- Useful for tuning background writer parameters to balance I/O load and cleaning effectiveness
- The underlying data comes from the background writer statistics structure maintained by the statistics collector
- Located in src/backend/utils/adt/pgstatfuncs.c:1225-1230