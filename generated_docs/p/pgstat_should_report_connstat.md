# pgstat_should_report_connstat

## Location
[src/backend/utils/activity/pgstat_database.c:324-332](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_database.c#L324-L332)

## Overview
pgstat_should_report_connstat is a static function that determines whether the current backend process should report connection statistics.

## Definition
static bool pgstat_should_report_connstat(void)

## Detailed Description
This function serves as a filter to determine which types of backend processes should contribute to session statistics reporting. It ensures that only normal backend processes (user connections) are included in session timing statistics, while excluding parallel workers and walsender processes. Parallel workers are excluded because they run in parallel and don't contribute meaningful session times, even though they consume CPU time. Walsender processes are excluded because they have different session characteristics (such as being always "active") that would skew the session statistics if included.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - B_BACKEND (backend type constant)
  - MyBackendType (global variable indicating current backend type)
- Called from (representative examples):
  - [pgstat_report_connect](pgstat_report_connect.md) (from src/backend/utils/activity/pgstat_database.c:195)
  - [pgstat_report_disconnect](pgstat_report_disconnect.md) (from src/backend/utils/activity/pgstat_database.c:212)
  - [pgstat_update_dbstats](pgstat_update_dbstats.md) (from src/backend/utils/activity/pgstat_database.c:292)

## Notes and Other Information
- The function is declared as static, meaning it's only accessible within the pgstat_database.c source file
- This filtering mechanism helps maintain the accuracy and relevance of session statistics by excluding process types that would introduce statistical noise
- The decision to exclude walsender processes is based on their always-active nature, which would skew session activity metrics
- This function is part of PostgreSQL's statistics collection system that ensures meaningful and accurate session timing data