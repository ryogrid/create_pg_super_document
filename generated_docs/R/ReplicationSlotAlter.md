# ReplicationSlotAlter

## Location
src/backend/replication/slot.c: 807 - 867

## Overview
Changes the definition of a replication slot identified by name, specifically allowing modification of the failover property with comprehensive validation checks.

## Definition
void ReplicationSlotAlter(const char *name, bool failover)

## Detailed Description
ReplicationSlotAlter provides functionality to modify the configuration of an existing logical replication slot, currently supporting changes to the failover property. The function performs extensive validation:

1. Ensures only logical replication slots can be altered (physical slots are not supported)
2. Prevents alteration of synced slots during recovery to maintain synchronization integrity  
3. Disallows enabling failover on standby servers since cascading standby synchronization is not supported
4. Prevents enabling failover for temporary slots as they are not synchronized to standbys
5. Only persists changes if the failover value actually differs from the current setting

The function follows the standard pattern of acquiring the slot, making validated modifications, persisting the changes, and releasing the slot.

## Parameters / Member Variables
- : The name of the replication slot to alter
- : Boolean flag indicating whether to enable or disable failover functionality for the slot

## Dependencies
- Functions called/Symbols referenced:
  - [ReplicationSlotAcquire](ReplicationSlotAcquire.md)
  - SlotIsPhysical
  - [RecoveryInProgress](RecoveryInProgress.md)
  - [ReplicationSlotMarkDirty](ReplicationSlotMarkDirty.md)
  - [ReplicationSlotSave](ReplicationSlotSave.md)
  - [ReplicationSlotRelease](ReplicationSlotRelease.md)
  - RS_TEMPORARY (slot persistency constant)
- Called from (representative examples):
  - [AlterReplicationSlot](../A/AlterReplicationSlot.md)

## Notes and Other Information
- Only supports logical replication slots - physical slots will generate an error
- Includes comprehensive validation for different deployment scenarios (primary/standby)
- Changes are only persisted if the new failover value differs from the current value
- Temporary slots cannot have failover enabled due to synchronization limitations
- Synced slots cannot be altered during recovery to maintain consistency
- The function properly handles slot acquisition and release for safe concurrent access
- Used primarily by the ALTER REPLICATION SLOT SQL command through the replication protocol