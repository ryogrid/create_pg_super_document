# LogicalRepWorkersWakeupAtCommit

## Location
src/backend/replication/logical/worker.c: 5065 - 5078

## Overview
Schedules logical replication workers for a given subscription to be woken up at the commit of the current transaction, ensuring prompt processing of subscription changes.

## Definition
void LogicalRepWorkersWakeupAtCommit(Oid subid)

## Detailed Description
This function is part of PostgreSQL's logical replication infrastructure that manages when subscription workers should be notified about changes. Rather than immediately waking up workers, it schedules them to be woken up when the current transaction commits successfully.

The function operates by:
1. Switching to TopTransactionContext to ensure the data persists for the transaction duration
2. Adding the subscription OID to a list of subscriptions whose workers need wakeup at commit
3. Using list_append_unique_oid() to avoid duplicate entries for the same subscription

This deferred wakeup mechanism is important because:
- It ensures workers only process changes from committed transactions
- It avoids unnecessary worker activity for transactions that might be rolled back
- It batches wakeup operations for efficiency
- It provides a centralized point for managing worker notifications

The actual wakeup of workers happens in the AtEOXact_LogicalRepWorkers() function, which is called at transaction end. If the transaction commits successfully, all subscription workers in the accumulated list will be woken up to process the new changes.

This design is commonly used when subscription metadata is modified (e.g., subscription renamed, owner changed, or other configuration updates) and the workers need to reload their configuration or process pending changes.

## Parameters / Member Variables
- : The Oid of the subscription whose workers should be woken up at transaction commit

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextSwitchTo (to switch to transaction context)
  - list_append_unique_oid (to add subscription OID to wakeup list)
- Called from (representative examples):
  - AlterObjectRename_internal (when subscription is renamed)
  - AlterSubscription (when subscription is modified)
  - AlterSubscriptionOwner_internal (when subscription owner changes)

## Notes and Other Information
- Uses TopTransactionContext to ensure the wakeup list survives until transaction end
- The on_commit_wakeup_workers_subids global list accumulates subscription OIDs during the transaction
- list_append_unique_oid ensures no duplicate entries for the same subscription
- The actual worker wakeup is performed by AtEOXact_LogicalRepWorkers at transaction commit
- Memory is automatically reclaimed at transaction end
- This function is declared in src/include/replication/logicalworker.h
- Located in src/backend/replication/logical/worker.c:5065-5078