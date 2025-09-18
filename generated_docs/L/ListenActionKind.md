# ListenActionKind

## Location
[src/backend/commands/async.c:337-342](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/async.c#L337-L342)

## Overview
ListenActionKind is an enumeration type that defines the possible actions for PostgreSQL's LISTEN/NOTIFY functionality, used to categorize different operations on notification channels.

## Definition


## Detailed Description
This enumeration is used within PostgreSQL's asynchronous notification system to distinguish between different types of operations that can be performed on notification channels. It serves as a type discriminator for pending listen/unlisten operations that are queued during a transaction and executed at commit time. The enum is part of the infrastructure that ensures transactional semantics for LISTEN and UNLISTEN commands.

## Parameters / Member Variables
- : Indicates a LISTEN operation to start listening on a specific channel
- : Indicates an UNLISTEN operation to stop listening on a specific channel  
- : Indicates an UNLISTEN * operation to stop listening on all channels

## Dependencies
- Functions called/Symbols referenced:
  - Used as a field type in ListenAction struct
- Called from (representative examples):
  - [queue_listen](../q/queue_listen.md) (src/backend/commands/async.c:690)
  - Referenced in NotificationHash and related notification processing functions

## Notes and Other Information
- This enum is used exclusively within the async.c module for managing LISTEN/NOTIFY operations
- The enum values correspond directly to the SQL commands LISTEN, UNLISTEN, and UNLISTEN *  
- Operations using this enum are queued during transaction execution and processed at commit time to ensure proper transactional behavior
- The enum is defined at src/backend/commands/async.c:332-337