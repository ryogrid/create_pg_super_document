# pq_beginmessage

## Location
src/backend/libpq/pqformat.c: 88 - 108

## Overview
Initializes a StringInfo buffer for sending a PostgreSQL protocol message by setting up the buffer and storing the message type.

## Definition

```c
void
pq_beginmessage(StringInfo buf, char msgtype)
```
## Detailed Description
This function prepares a StringInfo buffer for constructing a PostgreSQL protocol message. It initializes the buffer using  and cleverly stores the message type character in the buffer's cursor field rather than as the first byte of the message content. This design allows the message type to be preserved throughout the message construction process without interfering with the pq_sendXXX routines that will be used to populate the message content.

The function is part of PostgreSQL's internal protocol formatting system and is typically the first step when constructing any protocol message that will be sent from the backend to a client.

## Parameters / Member Variables
- : StringInfo buffer to be initialized for message construction
- : Single character identifying the type of PostgreSQL protocol message (e.g., 'Q' for Query, 'T' for RowDescription, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - initStringInfo (from StringInfo API)
- Called from (representative examples):
  - printsimple_startup
  - printsimple
  - bbsink_copystream_begin_archive
  - NotifyMyFrontEnd
  - ReceiveCopyBegin
  - SendCopyBegin
  - sendAuthRequest
  - ReadyForQuery
  - send_message_to_frontend
  - ReportGUCOption

## Notes and Other Information
- The message type is stored in the cursor field as a temporary holding place, expecting that subsequent pq_sendXXX routines won't modify this field
- This approach avoids having to manage the message type as the first byte of the buffer content during message construction
- Used extensively throughout PostgreSQL's backend for all client communication that follows the PostgreSQL wire protocol
- Must be followed by appropriate pq_sendXXX calls to build the message content and pq_endmessage to finalize the message