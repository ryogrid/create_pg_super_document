# ReceiveArchiveStream

## Location
src/bin/pg_basebackup/pg_basebackup.c: 1284 - 1331

## Overview
Receives all archives and backup manifest from the PostgreSQL server as a single COPY stream during pg_basebackup operations.

## Definition


## Detailed Description
ReceiveArchiveStream is the main coordinator function for receiving archive data during a base backup operation. It initializes the necessary state structure and delegates the actual data processing to ReceiveArchiveStreamChunk through the ReceiveCopyData mechanism. The function handles the complete lifecycle of archive stream processing including initialization, data reception, and cleanup.

The function manages several key aspects:
- Initializes ArchiveStreamState for tracking the stream processing state
- Coordinates the reception of data through ReceiveCopyData with ReceiveArchiveStreamChunk as the callback
- Handles backup manifest processing, either writing it to a file or injecting it into the output tarfile
- Performs cleanup of streaming resources including file handles and streamers

## Parameters / Member Variables
- : PostgreSQL database connection handle for receiving the COPY stream
- : Compression specification structure defining how data should be compressed during processing

## Dependencies
- Functions called/Symbols referenced:
  - [ReceiveCopyData](ReceiveCopyData.md)
  - [ReceiveArchiveStreamChunk](ReceiveArchiveStreamChunk.md)
  - [bbstreamer_inject_file](../b/bbstreamer_inject_file.md)
  - [bbstreamer_finalize](../b/bbstreamer_finalize.md)
  - [bbstreamer_free](../b/bbstreamer_free.md)
  - fclose
  - destroyPQExpBuffer
- Called from (representative examples):
  - [BaseBackup](../B/BaseBackup.md)

## Notes and Other Information
- This function is static and only used within pg_basebackup.c
- The function uses a callback-based approach where ReceiveArchiveStreamChunk processes individual data chunks
- Backup manifest handling is conditional and depends on whether the manifest is being written to a separate file or injected into the tarfile
- Proper cleanup is essential as the function manages file handles and streaming resources that must be properly closed/freed
- The tablespacenum is initialized to -1 indicating no specific tablespace is initially selected