# PhysicalConfirmReceivedLocation

## Location
src/backend/replication/walsender.c: 2369 - 2405

## Overview
Updates the restart LSN for a physical replication slot when a walreceiver confirms receipt of WAL data at a specific LSN, ensuring proper tracking of received data for WAL retention purposes.

## Definition


## Detailed Description
This function is called when a walreceiver (standby server) confirms that it has successfully received and processed WAL data up to a specific Log Sequence Number (LSN). The function updates the restart_lsn field of the current replication slot to reflect this confirmation, which is crucial for determining how much WAL data can be safely removed from the primary server.

The function operates under a spinlock to ensure atomic updates to the replication slot data. When the restart_lsn changes, it triggers several important maintenance operations: marking the slot as dirty for eventual persistence, recomputing the required LSN across all replication slots to determine WAL retention requirements, and waking up any logical WAL senders that might be waiting.

The design philosophy emphasizes performance over immediate durability - the slot is not immediately saved to disk since the worst-case scenario of losing this information would only result in more conservative WAL retention, not data loss.

## Parameters / Member Variables
- : The Log Sequence Number up to which the walreceiver has confirmed receipt of WAL data. Must not be InvalidXLogRecPtr.

## Dependencies
- Functions called/Symbols referenced:
  - [ReplicationSlot](../R/ReplicationSlot.md) (accessed via MyReplicationSlot global)
  - [ReplicationSlotMarkDirty](../R/ReplicationSlotMarkDirty.md)
  - [ReplicationSlotsComputeRequiredLSN](../R/ReplicationSlotsComputeRequiredLSN.md)  
  - [PhysicalWakeupLogicalWalSnd](PhysicalWakeupLogicalWalSnd.md)
- Called from (representative examples):
  - [ProcessStandbyReplyMessage](ProcessStandbyReplyMessage.md)

## Notes and Other Information
- The function uses spinlocks for thread-safe access to replication slot data
- Slot persistence is deliberately deferred for performance reasons - the checkpointer handles WAL segment retention based on restart_lsn
- The restart_lsn update triggers a chain of maintenance operations that affect WAL retention and logical replication coordination
- Located in src/backend/replication/walsender.c:2369-2405