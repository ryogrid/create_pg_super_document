# push_to_sink

## Location
[src/backend/backup/basebackup.c:1950-1990](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup.c#L1950-L1990)

## Overview
push_to_sink copies data into a bbsink's buffer with automatic flushing and checksum updating when the buffer becomes full.

## Definition
```c
static void push_to_sink(bbsink *sink, pg_checksum_context *checksum_ctx,
                        size_t *bytes_done, void *data, size_t length)
```

## Detailed Description
This function provides a mechanism to copy arbitrary data into a backup sink's buffer when direct buffer access is not practical. It manages buffer overflow by automatically flushing the buffer when it becomes full and updating the checksum context accordingly. The function uses a loop to handle cases where the data to be copied exceeds the available buffer space, breaking the operation into chunks and flushing as necessary.

The function is designed as a utility for cases where data cannot be read directly into the sink's buffer, providing buffered writing with transparent checksum maintenance and automatic buffer management.

## Parameters / Member Variables
- `sink`: bbsink object containing the destination buffer
- `checksum_ctx`: Pointer to checksum context for maintaining running checksum
- `bytes_done`: Pointer to counter tracking currently used bytes in buffer (updated by function)
- `data`: Pointer to source data to be copied into the buffer
- `length`: Number of bytes to copy from the source data

## Dependencies
- Functions called/Symbols referenced:
  - memcpy (standard library)
  - bbsink_archive_contents
  - pg_checksum_update
- Called from (representative examples):
  - [sendFile](../s/sendFile.md) (multiple calls for incremental backup headers)

## Notes and Other Information
- Uses < instead of <= comparison to trigger flush when data exactly fills remaining buffer space
- Automatically handles buffer overflow by flushing and continuing with remaining data
- Caller is responsible for performing final flush after all push_to_sink calls complete
- Primarily used for writing incremental backup headers and metadata that cannot be read directly into buffer
- Updates both the buffer content and maintains checksum state consistently
- Part of the backup sink abstraction layer in PostgreSQL's base backup system
- Located in src/backend/backup/basebackup.c:1950-1990