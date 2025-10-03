# UpdateSubscriptionRelState

## Location
[src/backend/catalog/pg_subscription.c:354-365](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_subscription.c#L354-L365)

## Overview
A simplified wrapper function that updates the replication state of a subscription table by calling the extended version with default locking behavior.

## Definition

```c
void
UpdateSubscriptionRelState(Oid subid, Oid relid, char state,
						   XLogRecPtr sublsn)
```
## Detailed Description
This function serves as a convenience wrapper around UpdateSubscriptionRelStateEx, providing a simpler interface for the most common use case of updating subscription relation state. It automatically handles lock acquisition by passing false for the already_locked parameter, making it suitable for contexts where the caller hasn't pre-acquired the necessary locks.

The function delegates all actual work to UpdateSubscriptionRelStateEx, maintaining consistency in the underlying implementation while providing a cleaner API for standard usage patterns.

## Parameters / Member Variables
- `subid`: The OID of the subscription containing the relation to update
- `relid`: The OID of the relation (table) whose state should be updated
- `state`: New character representing the replication state
- `sublsn`: New XLogRecPtr indicating the LSN position for replication tracking
## Dependencies
- Functions called/Symbols referenced:
  - [UpdateSubscriptionRelStateEx](UpdateSubscriptionRelStateEx.md)
- Called from (representative examples):
  - [process_syncing_tables_for_sync](../p/process_syncing_tables_for_sync.md)
  - [LogicalRepSyncTableStart](../L/LogicalRepSyncTableStart.md)

## Notes and Other Information
- Provides a simplified interface for the most common subscription state update scenarios
- Automatically manages locking by delegating to UpdateSubscriptionRelStateEx with already_locked=false
- Commonly used in logical replication table synchronization processes
- Located in src/backend/catalog/pg_subscription.c:354-365