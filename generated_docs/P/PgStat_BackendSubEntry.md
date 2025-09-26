# PgStat_BackendSubEntry

## Location
src/include/pgstat.h: 134 - 138

## Overview
PgStat_BackendSubEntry is a structure that stores non-flushed subscription statistics in backend local memory for logical replication subscriptions.

## Definition
```c
typedef struct PgStat_BackendSubEntry
{
    PgStat_Counter apply_error_count;
    PgStat_Counter sync_error_count;
} PgStat_BackendSubEntry;
```

## Detailed Description
This structure maintains error counts for logical replication subscriptions that are accumulated in backend local memory before being flushed to the statistics collector. It tracks two types of errors that can occur during logical replication: apply errors that happen during the application of changes from the publisher, and sync errors that occur during table synchronization operations. The structure is part of PostgreSQL's subscription statistics system and is used to monitor the health and performance of logical replication.

## Parameters / Member Variables
- : Counter tracking the number of errors encountered while applying replicated changes from the publisher
- : Counter tracking the number of errors encountered during table synchronization operations

## Dependencies
- Functions called/Symbols referenced:
  - PgStat_Counter (used for both counter fields)
- Called from (representative examples):
  - pgstat_report_subscription_error (error reporting)
  - pgstat_subscription_flush_cb (statistics flushing callback)

## Notes and Other Information
- Part of PostgreSQL's logical replication monitoring system
- Error counts are accumulated locally before being sent to the statistics collector
- Used to track subscription health and troubleshoot replication issues
- Located at src/include/pgstat.h:134-138
- Works in conjunction with the subscription statistics infrastructure