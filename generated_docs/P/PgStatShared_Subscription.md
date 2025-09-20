# PgStatShared_Subscription

## Location
[src/include/utils/pgstat_internal.h:404-408](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/pgstat_internal.h#L404-L408)

## Overview
PgStatShared_Subscription represents shared memory statistics data for a PostgreSQL logical replication subscription, providing counters for apply and sync errors along with reset timestamp information.

## Definition

```c
typedef struct PgStatShared_Subscription
{
	PgStatShared_Common header;
	PgStat_StatSubEntry stats;
} PgStatShared_Subscription;
```
## Detailed Description
This structure is used to store subscription-related statistics in PostgreSQL's shared memory statistics system. It combines a common header used by all shared statistics objects with subscription-specific statistical data. The structure is part of PostgreSQL's statistics collection infrastructure that tracks the performance and error states of logical replication subscriptions.

The structure follows the standard pattern for shared statistics objects in PostgreSQL, where each statistics object has a common header containing synchronization primitives and magic numbers for validation, followed by the actual statistical data specific to the object type.

## Parameters / Member Variables
- `header`: Common header structure (PgStatShared_Common) containing magic number for validation and an LWLock for protecting access to the statistics data
- `stats`: Subscription-specific statistics (PgStat_StatSubEntry) containing apply error count, sync error count, and statistical reset timestamp
## Dependencies
- Functions called/Symbols referenced:
  - PgStatShared_Common
  - [PgStat_StatSubEntry](PgStat_StatSubEntry.md)
- Called from (representative examples):
  - [pgstat_subscription_flush_cb](../p/pgstat_subscription_flush_cb.md)
  - [pgstat_subscription_reset_timestamp_cb](../p/pgstat_subscription_reset_timestamp_cb.md)
  - SH_DECLARE (hash table declarations in pgstat.c)

## Notes and Other Information
- Located in src/include/utils/pgstat_internal.h:404-408
- Part of PostgreSQL's shared memory statistics infrastructure for tracking logical replication subscription performance
- The structure is used within hash tables for efficient lookup of subscription statistics by subscription OID
- Access to the statistics data is protected by the LWLock in the header to ensure thread-safe operations in a multi-process environment
- The statistics are used for monitoring subscription health and performance, particularly tracking error conditions during logical replication