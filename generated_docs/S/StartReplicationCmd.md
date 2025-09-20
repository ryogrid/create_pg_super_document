# StartReplicationCmd

## Location
[src/include/nodes/replnodes.h:91-99](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/replnodes.h#L91-L99)

## Overview
StartReplicationCmd is a command structure used to initiate WAL replication from a PostgreSQL server, supporting both physical and logical replication modes.

## Definition

```c
typedef struct StartReplicationCmd
{
	NodeTag		type;
	ReplicationKind kind;
	char	   *slotname;
	TimeLineID	timeline;
	XLogRecPtr	startpoint;
	List	   *options;
} StartReplicationCmd;
```
## Detailed Description
StartReplicationCmd represents the START_REPLICATION command used in PostgreSQL's streaming replication protocol. This structure encapsulates all the necessary information to begin streaming WAL (Write-Ahead Log) records from a primary server to a standby or logical replication subscriber. The command supports both physical replication (for standby servers) and logical replication (for selective data synchronization).

## Parameters / Member Variables
- : NodeTag identifier for this command structure
- : ReplicationKind enum indicating whether this is physical or logical replication
- : Name of the replication slot to use for streaming (can be NULL for physical replication)
- : TimeLineID specifying which timeline to replicate from
- : XLogRecPtr indicating the WAL position to start replication from
- : List of additional options for the replication command

## Dependencies
- Functions called/Symbols referenced:
  - ReplicationKind
- Called from (representative examples):
  - [StartReplication](StartReplication.md) (src/backend/replication/walsender.c:823)
  - [StartLogicalReplication](StartLogicalReplication.md) (src/backend/replication/walsender.c:1456)
  - [exec_replication_command](../e/exec_replication_command.md) (src/backend/replication/walsender.c:2151)

## Notes and Other Information
- This structure is part of the replication protocol command set defined in replnodes.h
- The slotname field is particularly important for logical replication where slots track consumer progress
- The startpoint allows for resuming replication from a specific WAL position
- Options can include parameters like proto_version for logical replication compatibility