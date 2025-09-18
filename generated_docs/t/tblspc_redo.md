# tblspc_redo

## Location
[src/backend/commands/tablespace.c:1511-1569](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablespace.c#L1511-L1569)

## Overview
This function handles Write-Ahead Logging (WAL) redo operations for tablespace-related changes during PostgreSQL recovery, processing both tablespace creation and deletion operations.

## Definition


## Detailed Description
The  function is the WAL redo handler for the tablespace resource manager (RM_TBLSPC_ID). It processes WAL records related to tablespace operations during crash recovery, point-in-time recovery, or standby server replay. The function handles two main operation types:

1. **XLOG_TBLSPC_CREATE**: Recreates tablespace directories during recovery by calling  with the tablespace ID and path from the WAL record.

2. **XLOG_TBLSPC_DROP**: Handles tablespace deletion during recovery, which involves:
   - Closing all storage manager file descriptors across all backends using a process signal barrier
   - Attempting to destroy tablespace directories
   - If destruction fails (possibly due to temporary files from standby users), it resolves recovery conflicts and retries
   - On persistent failure, logs a warning rather than throwing an error to avoid crashing recovery

The function includes robust error handling for tablespace drop operations, recognizing that temporary files from standby users or permission issues might prevent immediate directory removal.

## Parameters / Member Variables
- : XLogReaderState pointer containing the WAL record to be replayed, including the operation type and associated data

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts operation type from WAL record
  - : Verifies no backup blocks (assertion)
  - : Retrieves record data payload
  - : Creates tablespace directory structure
  - : Emits process signal barrier
  - : Waits for signal barrier completion
  - : Removes tablespace directories
  - : Handles recovery conflicts
  - : Reports errors and warnings
  - : Logs panic messages for unknown opcodes
  - : WAL record struct for tablespace creation
  - : WAL record struct for tablespace deletion
  - : Operation code for tablespace creation
  - : Operation code for tablespace deletion
  - : Signal barrier type constant
- Called from:
  - PostgreSQL WAL replay system (registered in rmgrlist.h as part of tablespace resource manager)

## Notes and Other Information
- Registered as the redo function for RM_TBLSPC_ID in the resource manager list
- Does not use backup blocks (asserted at function start)
- Tablespace drop operations are designed to be fault-tolerant during recovery
- Uses process signal barriers to ensure clean shutdown of storage manager file descriptors
- Handles recovery conflicts gracefully when standby users have temporary files in the tablespace
- Logs warnings rather than errors for persistent directory removal failures to prevent recovery crashes
- Part of the PostgreSQL Write-Ahead Logging subsystem ensuring tablespace operations are crash-safe and recoverable