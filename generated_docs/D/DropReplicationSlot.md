# DropReplicationSlot

## Location
[src/bin/pg_basebackup/streamutil.c:763-811](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/streamutil.c#L763-L811)

## Overview
Removes an existing replication slot by name, with optional waiting behavior for active slots.

## Definition

```c
bool
DropReplicationSlot(PGconn *conn, const char *slot_name)
```
## Detailed Description
This function is a simple wrapper that drops a replication slot by calling the core ReplicationSlotDrop function. It translates the command parameters, particularly the wait flag, to control whether the operation should wait for an active slot to become inactive before dropping it, or fail immediately if the slot is currently in use.

## Parameters / Member Variables
- `cmd`: DropReplicationSlotCmd structure containing drop parameters including:
  - `slotname`: Name of the replication slot to drop
  - `wait`: Boolean flag indicating whether to wait for active slots (if true, waits; if false, fails immediately for active slots)

## Dependencies
- Functions called/Symbols referenced:
  - [ReplicationSlotDrop](../R/ReplicationSlotDrop.md) - Core function that performs the actual slot deletion
  - [DropReplicationSlotCmd](DropReplicationSlotCmd.md) - [Command](../C/Command.md) structure type
- Called from (representative examples):
  - [exec_replication_command](../e/exec_replication_command.md) (walsender.c:2138)
  - [main](../m/main.md) (pg_receivewal.c:880, pg_recvlogical.c:974)

## Notes and Other Information
- This is a thin wrapper function that delegates the actual work to ReplicationSlotDrop
- The wait parameter is inverted when passed to ReplicationSlotDrop (cmd->wait becomes !cmd->wait for the nowait parameter)
- If wait is false, the operation will fail if the slot is currently active/in use
- If wait is true, the operation will block until the slot becomes inactive, then drop it
- Used by both physical and logical replication slot management
- Part of the replication protocol command processing in WAL sender processes