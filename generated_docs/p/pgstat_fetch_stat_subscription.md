# pgstat_fetch_stat_subscription

## Location
[src/backend/utils/activity/pgstat_subscription.c:75-87](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_subscription.c#L75-L87)

## Overview
Retrieves the collected statistics for a specific logical replication subscription, serving as a support function for SQL-callable statistics functions.

## Definition

```c
PgStat_StatSubEntry *
pgstat_fetch_stat_subscription(Oid subid)
```
## Detailed Description
This function provides access to the accumulated statistics for a logical replication subscription. It serves as a bridge between the internal statistics collection system and the SQL-accessible statistics functions that users can query. The function retrieves the statistics entry for the specified subscription from the statistics collector's shared memory or cache.

The function returns a pointer to the statistics structure containing various counters and metrics for the subscription, such as error counts, last activity timestamps, and other operational metrics. If no statistics exist for the given subscription ID, the function returns NULL.

## Parameters / Member Variables
- : The OID of the subscription for which to fetch statistics

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_fetch_entry](pgstat_fetch_entry.md)
  - PGSTAT_KIND_SUBSCRIPTION
  - [PgStat_StatSubEntry](../P/PgStat_StatSubEntry.md)
- Called from (representative examples):
  - PG_STAT_GET_SUBSCRIPTION_STATS_COLS

## Notes and Other Information
This function is primarily used by PostgreSQL's built-in statistics functions that are accessible via SQL queries, such as pg_stat_subscription views. The returned PgStat_StatSubEntry structure contains detailed metrics about subscription performance and health, including apply and sync error counts, last error timestamps, and other operational data. The function is designed to be lightweight and efficient since it may be called frequently during statistics queries.