# PhysicalWakeupLogicalWalSnd

## Location
[src/backend/replication/walsender.c:1737-1761](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walsender.c#L1737-L1761)

## Overview
PhysicalWakeupLogicalWalSnd notifies logical WAL senders with failover slots when a synchronized physical replication slot advances, enabling coordinated failover behavior.

## Definition
```c
void PhysicalWakeupLogicalWalSnd(void)
```

## Detailed Description
This function is part of PostgreSQL`s logical replication failover mechanism. When a physical replication slot that is configured in the `synchronized_standby_slots` GUC parameter advances, this function wakes up logical WAL senders that have logical failover slots. This coordination ensures that logical replication slots can participate in failover scenarios by being kept synchronized with their corresponding slots on standby servers.

The function performs safety checks to ensure it only operates on physical slots and skips operation during recovery (since slot synchronization to cascading standbys is not supported). When conditions are met, it broadcasts a condition variable to wake up waiting logical WAL senders.

## Parameters / Member Variables
This function takes no parameters and operates on the current process`s replication slot (`MyReplicationSlot`).

## Dependencies
- Functions called/Symbols referenced:
  - SlotIsPhysical (verifies the current slot is a physical replication slot)
  - [RecoveryInProgress](../R/RecoveryInProgress.md) (checks if the database is in recovery mode)
  - [SlotExistsInSyncStandbySlots](../S/SlotExistsInSyncStandbySlots.md) (checks if slot is in synchronized_standby_slots GUC)
  - [ConditionVariableBroadcast](../C/ConditionVariableBroadcast.md) (wakes up waiting logical WAL senders)
- Called from (representative examples):
  - [pg_physical_replication_slot_advance](../p/pg_physical_replication_slot_advance.md) (when manually advancing physical slots)
  - [PhysicalConfirmReceivedLocation](PhysicalConfirmReceivedLocation.md) (when physical replication confirms WAL receipt)
  - [CRSSnapshotAction](../C/CRSSnapshotAction.md) (in replication slot snapshot handling)

## Dependencies
- Global variables accessed:
  - MyReplicationSlot (current process replication slot)
  - WalSndCtl->wal_confirm_rcv_cv (condition variable for WAL sender coordination)

## Notes and Other Information
- This function is critical for logical replication failover functionality introduced in recent PostgreSQL versions
- Only operates on primary servers (skips during recovery) since cascading standby slot sync is not supported
- Uses condition variable broadcasting for efficient notification of multiple waiting processes
- Part of the broader slot synchronization mechanism that enables logical replication to survive failover events
- Requires the synchronized_standby_slots GUC to be properly configured for the physical slot
- The function includes assertions to ensure it only operates on physical slots, preventing misuse