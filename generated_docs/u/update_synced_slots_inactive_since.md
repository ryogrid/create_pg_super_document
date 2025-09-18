# update_synced_slots_inactive_since

## Location
src/backend/replication/logical/slotsync.c: 1510 - 1561

## Overview
Updates the inactive_since timestamp for all synchronized replication slots during standby server shutdown to ensure accurate slot status tracking after potential promotion.

## Definition


## Detailed Description
This function is a critical component of PostgreSQL's logical replication slot management during server transitions. It iterates through all replication slots and updates the inactive_since timestamp for synchronized slots when the slot sync machinery is being shut down. The function is specifically designed to handle the scenario where a standby server is being promoted to primary, ensuring that slot inactivity timestamps are properly set to reflect the current time rather than potentially stale synchronization times. This prevents synchronized slots from appearing inactive for extended periods after promotion if they haven't been recently synchronized.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - GetCurrentTimestamp (PostgreSQL timestamp utility)
  - LWLockAcquire/LWLockRelease (lightweight lock management)
  - SpinLockAcquire/SpinLockRelease (spinlock for slot access)
  - SlotIsLogical (slot type validation)
  - StandbyMode (global standby status flag)
  - ReplicationSlotControlLock (global replication slot lock)
  - SlotSyncCtx (slot synchronization context)
  - ReplicationSlotCtl (replication slot control structure)

- Called from (representative examples):
  - ShutDownSlotSync (src/backend/replication/logical/slotsync.c:1577)
  - ShutDownSlotSync (src/backend/replication/logical/slotsync.c:1615)

## Notes and Other Information
- Only operates when in StandbyMode to avoid unnecessary processing on primary servers
- Ensures slot sync worker and SQL functions are not running before proceeding
- Uses a single timestamp for all slots being updated for consistency
- Acquires appropriate locks to ensure thread-safe access to slot structures
- Critical for proper slot state management during server promotion scenarios
- Part of the logical replication failover infrastructure
- Function is static and only accessible within the slotsync.c file
- Validates that synchronized slots are logical slots and not active