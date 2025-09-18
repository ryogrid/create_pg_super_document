# bbstreamer_tar_terminator_new

## Location
[src/bin/pg_basebackup/bbstreamer_tar.c:462-477](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_tar.c#L462-L477)

## Overview
Creates a bbstreamer that adds two blocks of NUL bytes to terminate incomplete tar archives from the server.

## Definition
```c
bbstreamer *bbstreamer_tar_terminator_new(bbstreamer *next)
```

## Detailed Description
This function creates a specialized bbstreamer designed to handle incomplete tar archives that might be sent by the PostgreSQL server. The tar format requires archives to end with two consecutive blocks (1024 bytes) of NUL (zero) bytes to properly terminate the archive. However, the server might send incomplete tar files that lack this proper termination.

The tar terminator bbstreamer ensures that any tar archive passing through it will have the correct termination by blindly appending two blocks of zero bytes to the end of the stream. This is a safety mechanism to guarantee that the resulting tar file will be properly formatted and readable by standard tar utilities.

Unlike the tar archiver, the terminator uses the basic bbstreamer structure rather than a specialized structure, as it doesn't need to maintain complex state.

## Parameters / Member Variables
- `next`: The next bbstreamer in the processing chain that will receive the terminated tar archive

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md) (PostgreSQL memory allocation)
  - bbstreamer_tar_terminator_ops (operations structure)
  - [bbstreamer](bbstreamer.md) (base streamer structure)
- Called from (representative examples):
  - [CreateBackupStreamer](../C/CreateBackupStreamer.md) (src/bin/pg_basebackup/pg_basebackup.c:1258)
  - [bbstreamer_buffer_until](bbstreamer_buffer_until.md) (src/bin/pg_basebackup/bbstreamer.h:217)

## Notes and Other Information
- The terminator blindly adds termination regardless of whether the archive already has proper termination - this ensures reliability but may result in extra zero blocks
- Uses the standard bbstreamer structure rather than a specialized one since no complex state tracking is needed
- Essential for ensuring tar archive compatibility when dealing with potentially incomplete server-generated archives
- Part of PostgreSQL's backup infrastructure to guarantee properly formatted tar output