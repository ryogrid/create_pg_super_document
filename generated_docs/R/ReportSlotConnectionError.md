# ReportSlotConnectionError

## Location
[src/backend/commands/subscriptioncmds.c:2248-2291](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/subscriptioncmds.c#L2248-L2291)

## Overview
Reports connection failures during replication slot cleanup operations and provides guidance for manual intervention when automatic slot dropping fails.

## Definition

```c
static void
ReportSlotConnectionError(List *rstates, Oid subid, char *slotname, char *err)
```
## Detailed Description
This function handles error reporting when PostgreSQL cannot connect to the publisher during replication slot cleanup operations, typically during subscription dropping. It iterates through all subscription relation states to identify tablesync slots that couldn't be automatically dropped, issuing warnings for each affected slot. The function then reports a comprehensive error message that includes the original connection failure details and provides actionable guidance for administrators to manually resolve the situation through SQL commands. This is crucial for maintaining system integrity when automatic cleanup fails due to network issues or publisher unavailability.

## Parameters / Member Variables
- : List of SubscriptionRelState structures representing the state of subscribed relations
- : OID of the subscription being processed
- : Name of the replication slot that failed to be dropped
- : Error message describing the connection failure

## Dependencies
- Functions called/Symbols referenced:
  - lfirst
  - OidIsValid
  - [ReplicationSlotNameForTablesync](ReplicationSlotNameForTablesync.md)
  - elog
  - ereport
- Called from (representative examples):
  - [DropSubscription](../D/DropSubscription.md)

## Notes and Other Information
- Only processes tablesync workers (relations with valid OIDs) and skips main subscription slots
- Focuses on relations that are not in SUBREL_STATE_SYNCDONE state, as these may have active tablesync slots
- Provides specific SQL command guidance (ALTER SUBSCRIPTION ... DISABLE and ALTER SUBSCRIPTION ... SET) for manual cleanup
- Uses WARNING level for individual tablesync slot issues and ERROR level for the overall connection failure
- Critical for preventing orphaned replication slots when subscription cleanup fails due to connectivity issues