# XLogPageRead

## Location
src/backend/access/transam/xlogrecovery.c: 3298 - 3541

## Overview
XLogPageRead is a critical function that reads WAL pages from various sources (local pg_wal, archive, or streaming) during PostgreSQL recovery, managing source switching, error handling, and nonblocking operations.

## Definition


## Detailed Description
XLogPageRead serves as the primary page reading mechanism for WAL recovery operations. It abstracts the complexity of reading WAL pages from multiple sources and handles the intricate logic of source switching when pages are unavailable. The function is designed to work in both blocking and non-blocking modes, supporting WAL prefetching operations.

Key responsibilities include:
- Reading WAL pages from the appropriate source (local files, archive, streaming)
- Managing segment file opening/closing and source transitions
- Handling checkpoint requests when too much WAL has been replayed
- Providing non-blocking operation support for WAL prefetching
- Validating page headers and handling corruption gracefully
- Supporting retry logic in standby mode

The function implements sophisticated error handling that differentiates between temporary failures (requiring retry) and permanent failures (requiring recovery termination).

## Parameters / Member Variables
- : XLogReaderState containing the reader context and configuration
- : XLogRecPtr specifying the WAL page location to read
- : Integer indicating the minimum number of bytes required
- : XLogRecPtr of the target record being read (for error reporting)
- : Character buffer where the read page data will be stored

## Dependencies
- Functions called/Symbols referenced:
  - WaitForWALToBecomeAvailable
  - XLogCheckpointNeeded
  - GetRedoRecPtr
  - RequestCheckpoint
  - emode_for_corrupt_record
  - XLogReaderValidatePageHeader
  - pg_pread
  - pgstat_report_wait_start/end
- Called from (representative examples):
  - InitWalRecovery (as page_read callback)
  - XLogReaderState callback mechanism

## Notes and Other Information
- Returns the number of bytes read on success, XLREAD_FAIL on permanent failure, or XLREAD_WOULDBLOCK for non-blocking operations
- Manages global variables readFile, readSegNo, readSource, readLen, and readOff to track current read state
- In standby mode, implements retry logic with source switching when pages are unavailable
- Page header validation occurs immediately to prevent issues with continuation records spanning different sources
- The function coordinates with the checkpoint system to request checkpoints when significant WAL has been consumed
- Non-blocking mode support enables efficient WAL prefetching without blocking the recovery process