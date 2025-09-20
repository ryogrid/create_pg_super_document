# pgstat_report_subscription_error

## Location
[src/backend/utils/activity/pgstat_subscription.c:27-45](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_subscription.c#L27-L45)

## Overview
Reports subscription-related errors to PostgreSQL's statistics collection system, tracking apply and sync error counts for logical replication subscriptions.

## Definition

```c
void
pgstat_report_subscription_error(Oid subid, bool is_apply_error)
```
## Detailed Description
This function is part of PostgreSQL's statistics collection system, specifically handling error reporting for logical replication subscriptions. It increments error counters in the pending statistics entry for a given subscription. The function distinguishes between two types of subscription errors: apply errors (when applying changes fails) and sync errors (when initial table synchronization fails).

The function works by preparing a pending statistics entry for the subscription and then incrementing the appropriate error counter based on the error type. This allows PostgreSQL to track subscription health and provide monitoring information about replication failures.

## Parameters / Member Variables
- : The OID of the subscription for which to report the error
- : Boolean flag indicating the error type - true for apply errors, false for sync errors

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_prep_pending_entry](pgstat_prep_pending_entry.md)
  - PGSTAT_KIND_SUBSCRIPTION
  - PgStat_EntryRef
  - PgStat_BackendSubEntry
- Called from (representative examples):
  - [start_table_sync](../s/start_table_sync.md)
  - [start_apply](../s/start_apply.md)
  - [DisableSubscriptionAndExit](../D/DisableSubscriptionAndExit.md)

## Notes and Other Information
This function is used in the logical replication worker processes to track errors during subscription operation. Apply errors occur during normal replication when applying changes from the publisher fails, while sync errors occur during initial table synchronization. The statistics collected here can be queried through PostgreSQL's statistics views to monitor subscription health and troubleshoot replication issues.