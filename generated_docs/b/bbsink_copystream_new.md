# bbsink_copystream_new

## Location
[src/backend/backup/basebackup_copy.c:108-125](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_copy.c#L108-L125)

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
  - [palloc0](../p/palloc0.md)
  - bbsink_copystream_ops
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - UINT64CONST
- Called from (representative examples):
  - [SendBaseBackup](../S/SendBaseBackup.md) (at src/backend/backup/basebackup.c:1032)

## Notes and Other Information
- The function initializes progress reporting with the current timestamp and zero bytes processed
- Uses a const cast to set the operations pointer, indicating the ops structure should not be modified after initialization
- The allocated bbsink_copystream structure includes a message buffer for protocol messages and progress tracking fields
- This sink type supports both archive content and manifest transmission through the same COPY stream
- Progress reporting is configured with predefined byte and time intervals to avoid excessive client updates

## Simplified Source

```c
// Simplified version of bbsink_copystream_new
bbsink *bbsink_copystream_new(bool send_to_client) {
    // Allocate and zero-initialize the copystream sink structure
    bbsink_copystream *sink = palloc0(sizeof(bbsink_copystream));

    // Set up the operations table for copystream functionality
    *((const bbsink_ops **) &sink->base.bbs_ops) = &bbsink_copystream_ops;

    // Store whether to send data to client
    sink->send_to_client = send_to_client;

    // Initialize progress reporting tracking
    sink->last_progress_report_time = GetCurrentTimestamp();
    sink->bytes_done_at_last_time_check = UINT64CONST(0);

    // Return the base sink interface
    return &sink->base;
}
```

Key simplifications made:
- Added clear comments explaining each initialization step
- Preserved all essential initialization logic
- Maintained the const cast pattern for operations table
- Kept progress reporting setup intact
- Simplified structure while preserving all functionality