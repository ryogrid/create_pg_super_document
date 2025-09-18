# EndReplicationCommand

## Location
src/backend/tcop/dest.c: 205 - 217

## Overview
EndReplicationCommand is a simplified version of EndCommand specifically designed for replication commands, sending completion status messages to replication clients.

## Definition
```c
void EndReplicationCommand(const char *commandTag)
```

## Detailed Description
EndReplicationCommand provides a stripped-down alternative to EndCommand that is optimized for replication scenarios. Unlike EndCommand which handles multiple destination types and QueryCompletion structures, this function directly accepts a simple command tag string and immediately sends a CommandComplete message to the client via the PostgreSQL protocol. This simplified approach is sufficient for replication commands which have more straightforward completion semantics.

The function is specifically designed for use by the replication subsystem and bypasses the more complex completion logic found in the general-purpose EndCommand function.

## Parameters / Member Variables
- `commandTag`: Null-terminated string containing the completion tag to be sent to the replication client

## Dependencies
- Functions called/Symbols referenced:
  - pq_putmessage (sends protocol message to client)
  - PqMsg_CommandComplete (protocol message type constant)
  - strlen (standard C library function for string length)
- Called from (representative examples):
  - [StartReplication](../S/StartReplication.md) (in walsender.c)
  - [exec_replication_command](../e/exec_replication_command.md) (multiple calls in walsender.c)

## Notes and Other Information
- Located in src/backend/tcop/dest.c:205-217
- Specifically designed for replication command completion
- Much simpler than EndCommand as it doesn't need to handle QueryCompletion structures or multiple destination types
- Used extensively throughout the replication subsystem (walsender.c)
- Part of the PostgreSQL replication protocol infrastructure
- Always sends messages to remote clients (no local destination handling needed)