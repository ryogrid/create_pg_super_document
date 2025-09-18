# pgstat_subscription_flush_cb

## Location
[src/backend/utils/activity/pgstat_subscription.c:88-100](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_subscription.c#L88-L100)

## Overview
Flushes pending subscription statistics from backend-local storage to the shared statistics area, handling error counts for logical replication subscriptions.

## Definition


## Detailed Description
This function is a callback used by the PostgreSQL statistics system to flush pending subscription statistics from backend-local storage to shared memory. It handles the transfer of subscription error statistics (both apply errors and sync errors) from the backend's pending entry to the shared statistics entry. The function implements locking mechanisms to ensure thread-safe access to shared statistics data.

The function performs atomic updates by acquiring a lock on the statistics entry, accumulating error counts from the local backend entry to the shared subscription entry, and then releasing the lock. This ensures consistent and thread-safe updates to subscription statistics across multiple backends.

## Parameters / Member Variables
- : Pointer to PgStat_EntryRef structure containing references to both pending (backend-local) and shared statistics entries
- : Boolean flag indicating whether to wait for the entry lock; if true and lock cannot be immediately acquired, the function returns false without flushing

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_lock_entry
  - pgstat_unlock_entry
  - PgStat_BackendSubEntry (type cast)
  - [PgStatShared_Subscription](../P/PgStatShared_Subscription.md) (type cast)
- Called from (representative examples):
  - PostgreSQL statistics system via SH_DECLARE macro registration

## Notes and Other Information
- The function uses a macro SUB_ACC to accumulate specific fields (apply_error_count and sync_error_count) from local to shared statistics
- Always assumes that localent has non-zero content, as indicated by the comment
- Returns true on successful flush, false only if nowait is true and the lock cannot be immediately acquired
- Part of PostgreSQL's modular statistics system for logical replication subscription monitoring
- Located in src/backend/utils/activity/pgstat_subscription.c:88-108