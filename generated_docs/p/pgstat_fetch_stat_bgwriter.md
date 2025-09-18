# pgstat_fetch_stat_bgwriter

## Location
[src/backend/utils/activity/pgstat_bgwriter.c:71-78](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_bgwriter.c#L71-L78)

## Overview
This function provides a way to access the background writer statistics for SQL-callable functions by returning a pointer to the bgwriter statistics structure.

## Definition


## Detailed Description
The  function serves as a support function for PostgreSQL's SQL-callable pgstat* functions. It ensures that the latest background writer statistics are available by taking a snapshot of the fixed statistics data and then returns a pointer to the bgwriter statistics structure from the local statistics snapshot. This function is essential for providing current statistical information about the background writer process to various PostgreSQL monitoring functions.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  -  (with PGSTAT_KIND_BGWRITER parameter)
  -  (constant defining bgwriter statistics type)
  -  (global structure containing bgwriter statistics)

- Called from (representative examples):
  -  (src/backend/utils/adt/pgstatfuncs.c:1221)
  -  (src/backend/utils/adt/pgstatfuncs.c:1227)
  -  (src/backend/utils/adt/pgstatfuncs.c:1255)
  -  (src/backend/utils/adt/pgstatfuncs.c:1261)

## Notes and Other Information
- This function is located in src/backend/utils/activity/pgstat_bgwriter.c:71-78
- The function ensures that statistics are current by calling  before returning the data
- The returned pointer references the bgwriter statistics from the local snapshot, providing consistent data for callers
- This function is primarily used by SQL-accessible functions that expose background writer statistics to users through system views and functions