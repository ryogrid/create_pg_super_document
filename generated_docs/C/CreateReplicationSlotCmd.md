# CreateReplicationSlotCmd

## Location
[src/include/nodes/replnodes.h:52-60](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/replnodes.h#L52-L60)

## Overview
CreateReplicationSlotCmd represents the CREATE_REPLICATION_SLOT replication protocol command, used to create new replication slots for both physical and logical replication.

## Definition
```c
typedef struct CreateReplicationSlotCmd
{
    NodeTag         type;
    char           *slotname;
    ReplicationKind kind;
    char           *plugin;
    bool            temporary;
    List           *options;
} CreateReplicationSlotCmd;
```

## Detailed Description
CreateReplicationSlotCmd encapsulates the CREATE_REPLICATION_SLOT replication command, which creates replication slots that track the progress of replication streams. Replication slots ensure that the WAL segments needed by a replica are retained on the primary server and prevent automatic cleanup until they are no longer needed.

The command supports two types of replication slots:
- **Physical slots** (REPLICATION_KIND_PHYSICAL): Used for streaming physical replication, tracking LSN positions
- **Logical slots** (REPLICATION_KIND_LOGICAL): Used for logical replication, requiring an output plugin to decode changes

When processed by the walsender, this command triggers the CreateReplicationSlot() function which handles the actual slot creation process. The function parses the provided options and calls the appropriate slot creation functions based on the replication kind.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a T_CreateReplicationSlotCmd node type
- `slotname`: Name of the replication slot to create (must be unique)
- `kind`: ReplicationKind enum value (REPLICATION_KIND_PHYSICAL or REPLICATION_KIND_LOGICAL)
- `plugin`: Output plugin name for logical replication slots (required for logical, NULL for physical)
- `temporary`: Boolean flag indicating whether the slot should be temporary (RS_TEMPORARY) or persistent (RS_PERSISTENT)
- `options`: List of DefElem structures containing additional slot configuration options (snapshot, reserve_wal, two_phase, failover)

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (from nodes/nodes.h)
  - ReplicationKind (enum from replnodes.h)
  - [List](../L/List.md) (from nodes/pg_list.h)
- Called from (representative examples):
  - walsender.c:2131 - [CreateReplicationSlot](CreateReplicationSlot.md)((CreateReplicationSlotCmd *) cmd_node)
  - walsender.c:1127 - [parseCreateReplSlotOptions](../p/parseCreateReplSlotOptions.md)() parses command options
  - Processed in replication command switch statement at walsender.c:2128

## Notes and Other Information
- Essential for establishing both physical and logical replication streams
- Replication slots persist across server restarts (unless marked as temporary)
- Physical slots track LSN positions and do not require output plugins
- Logical slots require an output plugin specified in the `plugin` field
- Options support advanced features like two-phase commit, failover capabilities, snapshot export control, and WAL reservation
- Part of the PostgreSQL streaming replication protocol
- Located in src/include/nodes/replnodes.h alongside other replication command structures
- Slot names must be unique across the entire cluster