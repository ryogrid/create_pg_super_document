# AlterReplicationSlotCmd

## Location
src/include/nodes/replnodes.h: 79 - 84

## Overview
AlterReplicationSlotCmd represents the ALTER_REPLICATION_SLOT replication protocol command, used to modify properties of existing replication slots.

## Definition
```c
typedef struct AlterReplicationSlotCmd
{
    NodeTag     type;
    char       *slotname;
    List       *options;
} AlterReplicationSlotCmd;
```

## Detailed Description
AlterReplicationSlotCmd encapsulates the ALTER_REPLICATION_SLOT replication command, which allows modification of existing replication slot properties without needing to drop and recreate the slot. This command provides a way to alter slot configuration while preserving the slot's state and position in the WAL stream.

The command supports modifying various slot properties through the options list. Currently, the primary supported option is:
- **failover**: Controls whether the slot can be used for failover scenarios in streaming replication setups

When processed by the walsender, this command triggers the AlterReplicationSlot() function, which parses the provided options via ParseAlterReplSlotOptions() and then calls ReplicationSlotAlter() to perform the actual slot modification.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a T_AlterReplicationSlotCmd node type
- `slotname`: Name of the existing replication slot to alter (must exist)
- `options`: List of DefElem structures containing slot modification options (currently supports "failover" boolean option)

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (from nodes/nodes.h)
  - List (from nodes/pg_list.h)
- Called from (representative examples):
  - walsender.c:2145 - AlterReplicationSlot((AlterReplicationSlotCmd *) cmd_node)
  - walsender.c:1418 - ParseAlterReplSlotOptions() parses command options
  - walsender.c:1448 - ReplicationSlotAlter(cmd->slotname, failover)
  - Processed in replication command switch statement at walsender.c:2142

## Notes and Other Information
- Provides a way to modify slot properties without losing slot state or position
- Currently focused on failover capability configuration for high availability scenarios
- More efficient than dropping and recreating slots since it preserves WAL position and state
- Works with both physical and logical replication slots
- Part of the PostgreSQL streaming replication protocol
- Located in src/include/nodes/replnodes.h alongside other replication command structures
- Options are parsed similarly to CREATE_REPLICATION_SLOT but with different supported parameters
- Essential for dynamic replication topology management and high availability configurations