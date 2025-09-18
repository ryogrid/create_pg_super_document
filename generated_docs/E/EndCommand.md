# EndCommand

## Location
src/backend/tcop/dest.c: 169 - 204

## Overview
EndCommand is responsible for clean up operations at the completion of a SQL command, primarily sending completion status messages to the client for remote destinations.

## Definition


## Detailed Description
EndCommand handles the final stage of command execution by processing command completion based on the destination type. For remote destinations (DestRemote, DestRemoteExecute, DestRemoteSimple), it builds a completion tag string using the QueryCompletion information and sends a CommandComplete message to the client via the PostgreSQL protocol. For other destination types (local, debug, SPI, etc.), no special completion processing is required.

The function uses a switch statement to handle different CommandDest values, with remote destinations requiring active completion message transmission while other destinations simply break through without additional processing.

## Parameters / Member Variables
- : Pointer to QueryCompletion structure containing command execution statistics and completion information
- : CommandDest enumeration value specifying where command output should be sent
- : Boolean flag to control completion tag formatting behavior

## Dependencies
- Functions called/Symbols referenced:
  - BuildQueryCompletionString (builds formatted completion tag)
  - pq_putmessage (sends protocol message to client)
  - CommandDest enum values (DestRemote, DestRemoteExecute, etc.)
  - QueryCompletion struct
  - PqMsg_CommandComplete (protocol message type)
- Called from (representative examples):
  - exec_simple_query (in postgres.c)
  - exec_execute_message (in postgres.c)
  - StartLogicalReplication (in walsender.c)
  - WalSndDone (in walsender.c)

## Notes and Other Information
- Located in src/backend/tcop/dest.c:169-204
- Part of the PostgreSQL command completion infrastructure
- Only remote destinations require active completion message handling
- Works in conjunction with BuildQueryCompletionString to format completion information
- Essential for proper client-server protocol communication in PostgreSQL