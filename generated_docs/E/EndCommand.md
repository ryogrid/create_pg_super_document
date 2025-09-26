# EndCommand

## Location
[src/backend/tcop/dest.c:169-204](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/dest.c#L169-L204)

## Overview
EndCommand is responsible for clean up operations at the completion of a SQL command, primarily sending completion status messages to the client for remote destinations.

## Definition

```c
void
EndCommand(const QueryCompletion *qc, CommandDest dest, bool force_undecorated_output)
```
## Detailed Description
EndCommand handles the final stage of command execution by processing command completion based on the destination type. For remote destinations (DestRemote, DestRemoteExecute, DestRemoteSimple), it builds a completion tag string using the QueryCompletion information and sends a CommandComplete message to the client via the PostgreSQL protocol. For other destination types (local, debug, SPI, etc.), no special completion processing is required.

The function uses a switch statement to handle different CommandDest values, with remote destinations requiring active completion message transmission while other destinations simply break through without additional processing.

## Parameters / Member Variables
- : Pointer to QueryCompletion structure containing command execution statistics and completion information
- : CommandDest enumeration value specifying where command output should be sent
- : Boolean flag to control completion tag formatting behavior

## Dependencies
- Functions called/Symbols referenced:
  - [BuildQueryCompletionString](../B/BuildQueryCompletionString.md) (builds formatted completion tag)
  - pq_putmessage (sends protocol message to client)
  - CommandDest enum values (DestRemote, DestRemoteExecute, etc.)
  - [QueryCompletion](../Q/QueryCompletion.md) struct
  - PqMsg_CommandComplete (protocol message type)
- Called from (representative examples):
  - [exec_simple_query](../e/exec_simple_query.md) (in postgres.c)
  - [exec_execute_message](../e/exec_execute_message.md) (in postgres.c)
  - [StartLogicalReplication](../S/StartLogicalReplication.md) (in walsender.c)
  - [WalSndDone](../W/WalSndDone.md) (in walsender.c)

## Notes and Other Information
- Located in src/backend/tcop/dest.c:169-204
- Part of the PostgreSQL command completion infrastructure
- Only remote destinations require active completion message handling
- Works in conjunction with BuildQueryCompletionString to format completion information
- Essential for proper client-server protocol communication in PostgreSQL