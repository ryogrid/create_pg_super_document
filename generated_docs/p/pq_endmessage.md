# pq_endmessage

## Location
src/backend/libpq/pqformat.c: 296 - 313

## Overview
Finalizes and sends a completed message buffer to the frontend client, cleaning up the StringInfo buffer afterwards.

## Definition


## Detailed Description
The pq_endmessage function serves as the final step in PostgreSQL's message construction and transmission process. After a message has been built using various pq_send* functions, this function takes the completed StringInfo buffer and transmits it to the connected client frontend.

The function performs two critical operations: first, it calls pq_putmessage() to actually send the message data over the network connection, using the message type that was previously stored in the buffer's cursor field. Second, it performs cleanup by freeing the buffer's data and setting the data pointer to NULL, preventing accidental reuse of the freed memory. Note that if the StringInfo structure itself was allocated with makeStringInfo, the caller remains responsible for freeing the StringInfo structure.

This function is designed to be the standard way to complete message transmission in PostgreSQL's client-server protocol, ensuring both proper network transmission and memory management.

## Parameters / Member Variables
- : StringInfo buffer containing the completed message to send and clean up

## Dependencies
- Functions called/Symbols referenced:
  - pq_putmessage (performs the actual network transmission of the message)
  - pfree (frees the buffer's data memory)

- Called from (representative examples):
  - ReadyForQuery (sends ready-for-query status messages)
  - SendCopyBegin, ReceiveCopyBegin (COPY operation setup)
  - NotifyMyFrontEnd (asynchronous notification system)
  - send_message_to_frontend (error and notice message transmission)
  - ReportGUCOption (GUC parameter reporting)
  - PostgresMain (various protocol messages in main query loop)

## Notes and Other Information
- The message type is retrieved from the cursor field of the StringInfo, which must be set prior to calling this function
- Automatically handles memory cleanup of the buffer data, but not the StringInfo structure itself
- Used extensively throughout PostgreSQL's client communication code
- Part of the standard message transmission pattern: pq_beginmessage → pq_send* functions → pq_endmessage
- Error handling for transmission failures is delegated to pqcomm.c - this function doesn't report transmission errors
- Critical for proper resource management in PostgreSQL's message-passing architecture
- The buffer becomes unusable after this call due to data pointer being set to NULL