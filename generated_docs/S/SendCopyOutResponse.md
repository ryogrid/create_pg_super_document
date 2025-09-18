# SendCopyOutResponse

## Location
src/backend/backup/basebackup_copy.c: 317 - 330

## Overview
SendCopyOutResponse is a static function that sends a CopyOutResponse message to initiate the PostgreSQL COPY protocol for data streaming during base backup operations.

## Definition


## Detailed Description
This function constructs and sends a CopyOutResponse message as part of the PostgreSQL frontend/backend protocol. It is specifically used in the context of base backup operations to inform the client that the server is ready to send data in COPY format. The function creates a message buffer, sets the overall format to 0 (text format), indicates 0 attributes (natts), and sends the complete message to the client.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - pq_beginmessage
  - PqMsg_CopyOutResponse
  - pq_sendbyte
  - pq_sendint16
  - pq_endmessage
- Called from (representative examples):
  - bbsink_copystream_begin_backup

## Notes and Other Information
- This is a static function limited to the basebackup_copy.c file
- The function sets the overall format to 0, indicating text format for the COPY operation
- The natts (number of attributes) is set to 0, which is typical for streaming operations
- Used specifically in base backup streaming operations where data is sent via the COPY protocol