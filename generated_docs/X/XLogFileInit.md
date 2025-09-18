# XLogFileInit

## Location
src/backend/access/transam/xlog.c: 3357 - 3394

## Overview
Creates a new XLOG file segment or opens a pre-existing one, providing a file descriptor for WAL operations within the PostgreSQL transaction logging system.

## Definition
int XLogFileInit(XLogSegNo logsegno, TimeLineID logtli)

## Detailed Description
XLogFileInit is a core function in PostgreSQL's Write-Ahead Logging (WAL) system responsible for initializing XLOG file segments. The function first attempts to create or reuse a WAL segment file through XLogFileInitInternal. If that operation returns a valid file descriptor, it's returned immediately. Otherwise, the function opens the target segment file directly using BasicOpenFile with appropriate flags for reading, writing, binary mode, close-on-exec, and WAL synchronization settings.

The function is designed to handle both critical and non-critical contexts - errors are reported as ERROR rather than PANIC, allowing the system to continue operating unless already in a critical section where they would be promoted to PANIC.

## Parameters / Member Variables
- : XLogSegNo identifying the specific WAL segment number to be created or opened
- : TimeLineID specifying the timeline for the log segment (must not be 0)

## Dependencies
- Functions called/Symbols referenced:
  - XLogFileInitInternal
  - BasicOpenFile
  - get_sync_bit
  - ereport
  - errcode_for_file_access
  - errmsg
- Called from (representative examples):
  - XLogWrite (src/backend/access/transam/xlog.c:2368)
  - BootStrapXLOG (src/backend/access/transam/xlog.c:5094)
  - XLogInitNewTimeline (src/backend/access/transam/xlog.c:5218)
  - XLogWalRcvWrite (src/backend/replication/walreceiver.c:929)

## Notes and Other Information
- The function asserts that logtli \!= 0 to ensure a valid timeline ID
- Uses file flags: O_RDWR | PG_BINARY | O_CLOEXEC along with WAL sync method-specific bits
- Error handling is designed to work both inside and outside critical sections
- Returns a file descriptor (>= 0) on success
- Located in src/backend/access/transam/xlog.c:3357-3394