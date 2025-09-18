# UpdateSubscriptionRelState

## Location
src/backend/catalog/pg_subscription.c: 354 - 365

## Overview
A simplified wrapper function that updates the replication state of a subscription table by calling the extended version with default locking behavior.

## Definition


## Detailed Description
This function serves as a convenience wrapper around UpdateSubscriptionRelStateEx, providing a simpler interface for the most common use case of updating subscription relation state. It automatically handles lock acquisition by passing false for the already_locked parameter, making it suitable for contexts where the caller hasn't pre-acquired the necessary locks.

The function delegates all actual work to UpdateSubscriptionRelStateEx, maintaining consistency in the underlying implementation while providing a cleaner API for standard usage patterns.

## Parameters / Member Variables
- : The OID of the subscription containing the relation to update
- : The OID of the relation (table) whose state should be updated
- : New character representing the replication state
- : New XLogRecPtr indicating the LSN position for replication tracking

## Dependencies
- Functions called/Symbols referenced:
  - UpdateSubscriptionRelStateEx
- Called from (representative examples):
  - process_syncing_tables_for_sync
  - LogicalRepSyncTableStart

## Notes and Other Information
- Provides a simplified interface for the most common subscription state update scenarios
- Automatically manages locking by delegating to UpdateSubscriptionRelStateEx with already_locked=false
- Commonly used in logical replication table synchronization processes
- Located in src/backend/catalog/pg_subscription.c:354-365