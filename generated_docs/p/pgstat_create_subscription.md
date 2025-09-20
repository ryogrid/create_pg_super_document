# pgstat_create_subscription

## Location
[src/backend/utils/activity/pgstat_subscription.c:46-63](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_subscription.c#L46-L63)

## Overview
Initializes statistics collection for a newly created logical replication subscription, setting up transactional tracking and creating a clean stats entry.

## Definition

```c
void
pgstat_create_subscription(Oid subid)
```
## Detailed Description
This function is called when a new logical replication subscription is created in PostgreSQL. It performs two key operations: first, it sets up transactional tracking to ensure that statistics are properly cleaned up if the transaction that creates the subscription rolls back; second, it creates and initializes a statistics entry for the subscription with reset counters.

The function ensures proper ACID compliance by using the transactional statistics mechanism, which means if the CREATE SUBSCRIPTION command fails or is rolled back, the statistics entry will be automatically cleaned up. After establishing transactional tracking, it creates the actual statistics entry and resets all counters to zero, providing a clean starting state for the new subscription.

## Parameters / Member Variables
- : The OID of the subscription being created

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_create_transactional
  - pgstat_get_entry_ref
  - pgstat_reset_entry
  - PGSTAT_KIND_SUBSCRIPTION
- Called from (representative examples):
  - [CreateSubscription](../C/CreateSubscription.md)

## Notes and Other Information
This function is typically called during the execution of a CREATE SUBSCRIPTION SQL command. The transactional nature ensures that incomplete subscription creations don't leave orphaned statistics entries. The function works in conjunction with the overall PostgreSQL statistics system to provide monitoring capabilities for logical replication subscriptions from the moment they are created.