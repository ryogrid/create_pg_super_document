# XLogReaderInvalReadState

## Location
src/backend/access/transam/xlogreader.c: 1123 - 1136

## Overview
Invalidates the XLogReaderState's cached read state to force re-reading from the underlying WAL source on the next access.

## Definition
```c
static void XLogReaderInvalReadState(XLogReaderState *state)
```

## Detailed Description
This function performs a lightweight invalidation of the reader's cached state by resetting the key fields that track what data is currently buffered. It clears the segment number, page offset, and read length to indicate that no valid cached data is available.

This invalidation is crucial for error recovery scenarios where the reader needs to restart from a clean state, or when the underlying data source may have changed. By resetting these fields to zero, subsequent read operations will be forced to fetch fresh data rather than relying on potentially stale or corrupted cached information.

The function is designed to be fast and simple, performing only the minimal state reset necessary to ensure data consistency without affecting other reader state like decode queues or position tracking.

## Parameters / Member Variables
- `state`: XLogReaderState whose read cache should be invalidated

## Dependencies
- Functions called/Symbols referenced:
  - (None - direct field assignments only)
- Called from (representative examples):
  - XLogDecodeNextRecord
  - ReadPageInternal
  - XLogFindNextRecord

## Notes and Other Information
- Resets ws_segno, segoff, and readLen to 0 to indicate invalid/empty cache
- Does not affect decode queues, error state, or position tracking fields
- Called primarily during error recovery to ensure clean restart
- Lightweight operation that only touches the minimal state needed for cache invalidation
- Subsequent read operations will be forced to fetch fresh data from the WAL source
- Used when switching between different WAL sources or when corruption is detected