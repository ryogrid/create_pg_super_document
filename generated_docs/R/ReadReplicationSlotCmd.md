# ReadReplicationSlotCmd

## Location
[src/include/nodes/replnodes.h:106-110](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/replnodes.h#L106-L110)

## Overview
ReadReplicationSlotCmd is a command structure used to read information about a specific replication slot in PostgreSQL's replication system.

## Definition
```c
typedef struct ReadReplicationSlotCmd
{
    NodeTag      type;
    char        *slotname;
} ReadReplicationSlotCmd;
```

## Detailed Description
ReadReplicationSlotCmd represents the READ_REPLICATION_SLOT command in PostgreSQL's streaming replication protocol. This structure is used to retrieve information about a specific replication slot, including its current state, position, and other metadata. This command is essential for monitoring and managing replication slots, allowing clients to inspect slot properties without modifying them.

## Parameters / Member Variables
- `type`: NodeTag identifier for this command structure
- `slotname`: Name of the replication slot to read information from

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references)
- Called from (representative examples):
  - ReadReplicationSlot (src/backend/replication/walsender.c:494)
  - [exec_replication_command](../e/exec_replication_command.md) (src/backend/replication/walsender.c:2116)

## Notes and Other Information
- This is a read-only operation that does not modify the replication slot
- The command returns detailed information about the slot including its type, position, and status
- Essential for replication monitoring and slot management operations
- Part of the replication protocol command set defined in replnodes.h