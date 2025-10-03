# SendCopyOutResponse

## Location
[src/backend/backup/basebackup_copy.c:317-330](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_copy.c#L317-L330)

## Overview
SendCopyOutResponse is a static function that sends a CopyOutResponse message to initiate the PostgreSQL COPY protocol for data streaming during base backup operations.

## Definition

```c
static void
SendCopyOutResponse(void)
```
## Detailed Description
This function constructs and sends a CopyOutResponse message as part of the PostgreSQL frontend/backend protocol. It is specifically used in the context of base backup operations to inform the client that the server is ready to send data in COPY format. The function creates a message buffer, sets the overall format to 0 (text format), indicates 0 attributes (natts), and sends the complete message to the client.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [pq_beginmessage](../p/pq_beginmessage.md)
  - PqMsg_CopyOutResponse
  - [pq_sendbyte](../p/pq_sendbyte.md)
  - [pq_sendint16](../p/pq_sendint16.md)
  - [pq_endmessage](../p/pq_endmessage.md)
- Called from (representative examples):
  - [bbsink_copystream_begin_backup](../b/bbsink_copystream_begin_backup.md)

## Notes and Other Information
- This is a static function limited to the basebackup_copy.c file
- The function sets the overall format to 0, indicating text format for the COPY operation
- The natts (number of attributes) is set to 0, which is typical for streaming operations
- Used specifically in base backup streaming operations where data is sent via the COPY protocol