# ReceiveArchiveStreamChunk

## Location
src/bin/pg_basebackup/pg_basebackup.c: 1332 - 1515

## Overview
Processes individual data chunks received as part of a COPY stream during archive reception, handling different message types for archives, manifest data, and progress reports.

## Definition


## Detailed Description
ReceiveArchiveStreamChunk is a callback function that processes individual chunks of data received through the COPY protocol during base backup operations. It acts as a message dispatcher, examining the type byte of each CopyData message and routing the processing accordingly.

The function handles five distinct message types:
- 'n' (New archive): Initializes processing for a new archive, including tablespace validation and streamer setup
- 'd' (Data): Processes actual archive or manifest content data
- 'p' (Progress): Updates progress tracking with byte counts from the server
- 'm' (Manifest): Prepares for receiving backup manifest data
- Default: Reports parsing errors for unrecognized message types

The function manages the complete lifecycle of archive processing, from initialization through data streaming to cleanup, while maintaining proper state transitions and error handling.

## Parameters / Member Variables
- : Size of the data chunk received in the copybuf
- : Buffer containing the raw COPY data received from the server
- : Void pointer to ArchiveStreamState structure containing processing state

## Dependencies
- Functions called/Symbols referenced:
  - [GetCopyDataByte](../G/GetCopyDataByte.md)
  - [GetCopyDataString](../G/GetCopyDataString.md)
  - [GetCopyDataUInt64](../G/GetCopyDataUInt64.md)
  - [GetCopyDataEnd](../G/GetCopyDataEnd.md)
  - [progress_report](../p/progress_report.md)
  - [bbstreamer_finalize](../b/bbstreamer_finalize.md)
  - [bbstreamer_free](../b/bbstreamer_free.md)
  - [bbstreamer_content](../b/bbstreamer_content.md)
  - [CreateBackupStreamer](../C/CreateBackupStreamer.md)
  - [ReportCopyDataParseError](ReportCopyDataParseError.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - createPQExpBuffer
  - fopen
  - fwrite
- Called from (representative examples):
  - [ReceiveArchiveStream](ReceiveArchiveStream.md)
  - CompressionLocation

## Notes and Other Information
- This function implements a state machine that processes different phases of archive reception
- [Archive](../A/Archive.md) name validation prevents directory traversal and ensures safe file handling
- Progress reporting is forced on each tablespace transition and server progress message
- Manifest data can be either buffered in memory for injection into tarfiles or written directly to disk
- The function assumes PostgreSQL v15+ protocol features (recovery GUCs support)
- Proper error handling includes setting errno to ENOSPC when fwrite() fails without setting errno
- State management ensures that archives are processed before manifest data (sanity check enforced)