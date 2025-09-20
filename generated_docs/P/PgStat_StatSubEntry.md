# PgStat_StatSubEntry

## Location
[src/include/pgstat.h:392-397](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/pgstat.h#L392-L397)

## Overview
PgStat_StatSubEntry is a structure that holds error statistics for PostgreSQL logical replication subscriptions, tracking apply and sync error counts along with the timestamp of the last statistics reset.

## Definition

```c
typedef struct PgStat_StatSubEntry
{
	PgStat_Counter apply_error_count;
	PgStat_Counter sync_error_count;
	TimestampTz stat_reset_timestamp;
} PgStat_StatSubEntry;
```
## Detailed Description
This structure is part of PostgreSQL's statistics collection system, specifically designed for tracking error statistics related to logical replication subscriptions. It maintains counters for different types of errors that can occur during the replication process and provides a timestamp indicating when the statistics were last reset. This information is crucial for monitoring the health and performance of logical replication subscriptions.

## Parameters / Member Variables
- : Counter tracking the number of errors that occurred during the apply phase of logical replication
- : Counter tracking the number of errors that occurred during the sync phase of logical replication  
- : Timestamp indicating when these statistics were last reset to zero

## Dependencies
- Functions called/Symbols referenced:
  - PgStat_Counter (used for counter fields)
  - TimestampTz (used for timestamp field)
- Called from (representative examples):
  - [pgstat_drop_subscription](../p/pgstat_drop_subscription.md)
  - [pgstat_fetch_stat_subscription](../p/pgstat_fetch_stat_subscription.md)
  - PG_STAT_GET_SUBSCRIPTION_STATS_COLS
  - [PgStatShared_Subscription](PgStatShared_Subscription.md)

## Notes and Other Information
- This structure is defined in src/include/pgstat.h at lines 392-397
- It's primarily used in the PostgreSQL statistics system for subscription monitoring
- The structure provides essential metrics for diagnosing issues with logical replication subscriptions
- Statistics can be viewed through system functions and are reset when explicitly requested or during certain maintenance operations