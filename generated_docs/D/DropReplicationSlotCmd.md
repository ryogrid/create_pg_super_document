# DropReplicationSlotCmd

## Location
src/include/nodes/replnodes.h: 67 - 72

## Overview
DropReplicationSlotCmd represents the DROP_REPLICATION_SLOT replication protocol command, used to delete existing replication slots from the PostgreSQL server.

## Definition
```c
typedef struct DropReplicationSlotCmd
{
    NodeTag     type;
    char       *slotname;
    bool        wait;
} DropReplicationSlotCmd;
```

## Detailed Description
DropReplicationSlotCmd encapsulates the DROP_REPLICATION_SLOT replication command, which removes replication slots that are no longer needed. Replication slots track the progress of replication streams and prevent WAL segments from being automatically cleaned up. When a slot is dropped, PostgreSQL can reclaim the disk space used by WAL segments that were being retained for that slot.

The command supports both immediate and waiting drop modes:
- **Immediate drop** (wait=false): Attempts to drop the slot immediately, which may fail if the slot is currently active
- **Waiting drop** (wait=true): Waits for the slot to become inactive before dropping it

When processed by the walsender, this command triggers the DropReplicationSlot() function which calls ReplicationSlotDrop() with the appropriate parameters based on the wait flag.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a T_DropReplicationSlotCmd node type
- `slotname`: Name of the replication slot to drop (must exist)
- `wait`: Boolean flag controlling drop behavior - if false, drops immediately (may fail if slot is active); if true, waits for slot to become inactive before dropping

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (from nodes/nodes.h)
- Called from (representative examples):
  - walsender.c:2138 - [DropReplicationSlot](DropReplicationSlot.md)((DropReplicationSlotCmd *) cmd_node)
  - walsender.c:1411 - [ReplicationSlotDrop](../R/ReplicationSlotDrop.md)(cmd->slotname, !cmd->wait)
  - Processed in replication command switch statement at walsender.c:2135

## Notes and Other Information
- Essential for cleanup and management of replication infrastructure
- Dropping active slots without waiting can fail, so the wait parameter provides safe cleanup
- Once dropped, the slot cannot be recovered and any replica depending on it will need to be reinitialized
- Dropping a slot allows PostgreSQL to reclaim disk space from retained WAL segments
- Works with both physical and logical replication slots
- Part of the PostgreSQL streaming replication protocol
- Located in src/include/nodes/replnodes.h alongside other replication command structures
- The wait parameter maps inversely to the nowait parameter in ReplicationSlotDrop()