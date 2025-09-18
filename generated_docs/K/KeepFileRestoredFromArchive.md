# KeepFileRestoredFromArchive

## Location
src/backend/access/transam/xlogarchive.c: 358 - 443

## Overview
Moves a file restored from archive storage from its temporary location to the permanent location in pg_wal, handling file replacement and necessary cleanup operations.

## Definition


## Detailed Description
KeepFileRestoredFromArchive finalizes the restoration of an archived file by moving it from its temporary restoration path to the permanent location in the pg_wal directory. The function handles the complexities of file replacement, particularly on Windows systems where open file handles can prevent immediate replacement.

The function implements platform-specific logic for safe file replacement - on Windows, it uses a unique temporary naming scheme to work around file locking issues with processes that may have the old file open. It also manages archive notification to prevent the restored file from being archived again and coordinates with WAL senders to ensure they reload the new file content.

After successfully moving the file, the function creates appropriate archive status files and signals relevant processes about the availability of new WAL data.

## Parameters / Member Variables
- : The temporary path where the restored file currently exists
- : The target filename in pg_wal directory (without directory path)

## Dependencies
- Functions called/Symbols referenced:
  - rename: Renames files (Windows-specific temporary renaming)
  - strlcpy: Safe string copying (non-Windows platforms)
  - unlink: Removes old files
  - durable_rename: Performs atomic file rename with fsync
  - XLogArchiveForceDone: Creates .done file when not in ALWAYS archive mode
  - XLogArchiveNotify: Creates .ready file in ALWAYS archive mode
  - WalSndRqstFileReload: Requests WAL senders to reload the current segment
  - WalSndWakeup: Signals WAL senders about new WAL availability
- Called from (representative examples):
  - XLogFileRead: After successfully restoring a WAL file during recovery
  - restoreTimeLineHistoryFiles: When keeping restored timeline history files
  - readTimeLineHistory: During timeline history file processing

## Notes and Other Information
- Implements Windows-specific workarounds for file locking issues using unique deletion counters
- Handles archive mode appropriately - creates .done files in non-ALWAYS mode, .ready files in ALWAYS mode
- Ensures WAL senders are properly notified about file changes through reload requests and wakeup signals
- Uses durable_rename to ensure atomic file replacement with proper synchronization
- Critical for maintaining consistency between restored files and active WAL processing
- The reload mechanism is essential for preventing WAL senders from serving stale data after file replacement