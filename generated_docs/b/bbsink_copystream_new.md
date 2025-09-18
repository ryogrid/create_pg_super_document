# bbsink_copystream_new

## Location
src/backend/backup/basebackup_copy.c: 108 - 125

## Overview
Creates a new 'copystream' basebackup sink that sends backup archives via PostgreSQL's COPY protocol to clients or other destinations.

## Definition
bbsink *bbsink_copystream_new(bool send_to_client)

## Detailed Description
This function creates and initializes a new bbsink_copystream instance, which is a specialized base backup sink implementation that transmits backup data using PostgreSQL's COPY OUT protocol. The function allocates memory for the sink structure, sets up the operations table with copystream-specific handlers, and initializes progress reporting mechanisms. The resulting sink can send backup archives and manifest data through a single COPY stream, with each CopyData message prefixed by a type byte to distinguish different content types.

## Parameters / Member Variables
- : Boolean flag indicating whether the archives should be sent to the client (true) or handled differently (false)

## Dependencies
- Functions called/Symbols referenced:
  - palloc0
  - bbsink_copystream_ops
  - GetCurrentTimestamp
  - UINT64CONST
- Called from (representative examples):
  - SendBaseBackup (at src/backend/backup/basebackup.c:1032)

## Notes and Other Information
- The function initializes progress reporting with the current timestamp and zero bytes processed
- Uses a const cast to set the operations pointer, indicating the ops structure should not be modified after initialization
- The allocated bbsink_copystream structure includes a message buffer for protocol messages and progress tracking fields
- This sink type supports both archive content and manifest transmission through the same COPY stream
- Progress reporting is configured with predefined byte and time intervals to avoid excessive client updates