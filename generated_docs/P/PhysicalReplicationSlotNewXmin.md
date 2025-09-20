# PhysicalReplicationSlotNewXmin

## Location
[src/backend/replication/walsender.c:2511-2559](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walsender.c#L2511-L2559)

## Overview
Updates the xmin horizon for a physical replication slot based on hot standby feedback, ensuring proper coordination between primary and standby servers for transaction visibility and VACUUM operations.

## Definition

```c
static void
PhysicalReplicationSlotNewXmin(TransactionId feedbackXmin, TransactionId feedbackCatalogXmin)
```
## Detailed Description
This function processes hot standby feedback messages that inform the primary server about the oldest transaction IDs still visible on the standby server. It updates both the regular xmin and catalog_xmin values in the replication slot to prevent the primary from removing tuples that are still needed by queries running on the standby.

The function operates under a spinlock to ensure atomic updates to the replication slot data. For physical replication, it sets both the data and effective xmin values simultaneously since the interlocking mechanisms used by logical replication are not needed - the only consequence of a missed xmin increase would be query cancellations rather than data corruption.

When either xmin value changes, the function marks the slot as dirty for eventual persistence and triggers a recomputation of the required xmin across all replication slots, which affects VACUUM behavior on the primary server.

## Parameters / Member Variables
- : The oldest transaction ID still visible to regular queries on the standby server
- : The oldest transaction ID still visible to catalog queries on the standby server

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdIsNormal (transaction ID validation)
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md) (transaction ID comparison)
  - [ReplicationSlot](../R/ReplicationSlot.md) (accessed via MyReplicationSlot global)
  - [ReplicationSlotMarkDirty](../R/ReplicationSlotMarkDirty.md) (slot persistence)
  - [ReplicationSlotsComputeRequiredXmin](../R/ReplicationSlotsComputeRequiredXmin.md) (global xmin computation)
- Called from (representative examples):
  - [ProcessStandbyHSFeedbackMessage](ProcessStandbyHSFeedbackMessage.md)

## Notes and Other Information
- Uses spinlocks for thread-safe access to replication slot data
- Sets MyProc->xmin to InvalidTransactionId to clear any local xmin constraints
- Physical replication slots don't require the complex interlocking used by logical slots
- Updates both data and effective xmin values atomically for consistency
- Critical for preventing premature tuple removal on primary that could break standby queries
- Handles both normal transaction IDs and special values (bootstrap, frozen) appropriately
- Located in src/backend/replication/walsender.c:2511-2559