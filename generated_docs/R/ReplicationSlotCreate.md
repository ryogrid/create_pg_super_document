# ReplicationSlotCreate

## Location
[src/backend/replication/slot.c:309-463](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L309-L463)

## Overview
Creates a new replication slot and marks it as used by the current backend, handling both physical and logical replication slots with various configuration options.

## Definition


## Detailed Description
ReplicationSlotCreate is responsible for creating and initializing a new replication slot in PostgreSQL. It performs comprehensive validation, handles resource allocation, and ensures thread-safe creation with proper locking mechanisms. The function supports various replication slot types including physical slots for streaming replication and logical slots for logical decoding, with advanced features like two-phase commit support and failover capabilities.

The function implements strict validation rules to prevent invalid configurations, such as disallowing failover-enabled slots on standby servers (except during slot synchronization) and preventing temporary failover slots. It uses a combination of ReplicationSlotAllocationLock and ReplicationSlotControlLock to ensure atomic slot creation and prevent race conditions.

## Parameters / Member Variables
- : The name of the replication slot to create (must be unique)
- : If true, the slot is database-specific for logical decoding; if false, it's for physical replication
- : Determines slot persistence (RS_PERSISTENT, RS_EPHEMERAL, or RS_TEMPORARY)
- : Enables decoding of prepared transactions for logical slots (can only be set at creation time)
- : Enables slot synchronization to standbys for logical replication continuity after failover
- : Indicates if the slot is synchronized from a primary server

## Dependencies
- Functions called/Symbols referenced:
  - [ReplicationSlotValidateName](ReplicationSlotValidateName.md)
  - [RecoveryInProgress](RecoveryInProgress.md)
  - IsSyncingReplicationSlots
  - LWLockAcquire/LWLockRelease
  - [CreateSlotOnDisk](../C/CreateSlotOnDisk.md)
  - pgstat_create_replslot
  - ConditionVariableBroadcast
- Called from (representative examples):
  - [create_physical_replication_slot](../c/create_physical_replication_slot.md)
  - [create_logical_replication_slot](../c/create_logical_replication_slot.md)
  - [CreateReplicationSlot](../C/CreateReplicationSlot.md) (walsender)
  - [synchronize_one_slot](../s/synchronize_one_slot.md)

## Notes and Other Information
- The function ensures atomic slot creation using exclusive locking on ReplicationSlotAllocationLock
- Validates slot name uniqueness and enforces max_replication_slots limit
- Sets MyReplicationSlot global variable to the newly created slot
- Creates statistics entries only for logical slots
- Two-phase commit option cannot be changed after slot creation to maintain consistency
- Failover slots cannot be created on standby servers except during slot synchronization process
- The slot is marked as active with the current process PID upon successful creation