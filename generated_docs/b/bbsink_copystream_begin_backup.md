# bbsink_copystream_begin_backup

## Location
[src/backend/backup/basebackup_copy.c:126-164](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_copy.c#L126-L164)

## Overview
Initializes a copystream basebackup sink by setting up protocol buffers and sending initial backup wire protocol messages to the client.

## Definition
static void bbsink_copystream_begin_backup(bbsink *sink)

## Detailed Description
This function performs the initial setup when starting a basebackup operation using the copystream sink. It allocates and configures the message buffer with proper alignment for CopyData protocol messages, where each message payload begins with a type byte ('d' for archive or manifest data). The function then sends the backup start location to the client, transmits the tablespace list, sends a CommandComplete message, and begins the COPY OUT stream that will be used for all archives and manifest data.

## Parameters / Member Variables
- : Pointer to the base bbsink structure (cast to bbsink_copystream internally)

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md)
  - [SendXlogRecPtrResult](../S/SendXlogRecPtrResult.md)
  - [SendTablespaceList](../S/SendTablespaceList.md)  
  - [pq_puttextmessage](../p/pq_puttextmessage.md)
  - [SendCopyOutResponse](../S/SendCopyOutResponse.md)
  - MAXIMUM_ALIGNOF
  - PqMsg_CommandComplete
- Called from (representative examples):
  - Referenced by bbsink_copystream_ops structure as the begin_backup handler

## Notes and Other Information
- Allocates a message buffer with extra space for alignment requirements, ensuring the buffer exposed to callers is properly aligned while leaving room for the protocol type byte
- Sets the first character of the message buffer to 'd' to indicate archive or manifest data
- The buffer allocation accounts for MAXIMUM_ALIGNOF to ensure proper memory alignment for performance
- Sends three initial protocol messages: backup start location, tablespace information, and begins the COPY OUT response
- All subsequent archive and manifest data will use the single COPY stream initialized here