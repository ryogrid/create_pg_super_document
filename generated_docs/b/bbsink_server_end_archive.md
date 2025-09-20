# bbsink_server_end_archive

## Location
[src/backend/backup/basebackup_server.c:194-227](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_server.c#L194-L227)

## Overview
Finalizes the current archive file by syncing it to disk, closing the file, and cleaning up the sink state.

## Definition

```c
static void
bbsink_server_end_archive(bbsink *sink)
```
## Detailed Description
This function completes the archive writing process by ensuring data durability and proper cleanup. It performs an fsync operation to guarantee the backup data is written to persistent storage before closing the file. The function uses a conservative error handling approach, treating sync failures as errors rather than causing server panic, since backup failures don't require database recovery.

After successful synchronization, the function closes the file and resets the sink's file handle and position tracking to prepare for the next archive operation.

## Parameters / Member Variables
- : Pointer to the bbsink instance (cast to bbsink_server internally)

## Dependencies
- Functions called/Symbols referenced:
  - FileSync
  - [FilePathName](../F/FilePathName.md)
  - FileClose  
  - ereport
  - [errcode_for_file_access](../e/errcode_for_file_access.md)
  - [errmsg](../e/errmsg.md)
  - [bbsink_forward_end_archive](bbsink_forward_end_archive.md)
- Called from (representative examples):
  - Referenced through bbsink_server_ops function table

## Notes and Other Information
- Uses WAIT_EVENT_BASEBACKUP_SYNC for proper wait event monitoring during fsync
- Intentionally avoids data_sync_elevel to prevent server PANIC on sync failures
- Resets mysink->file to 0 and mysink->filepos to 0 after closing
- Conservative error handling: sync failures are ERRORs, not PANIC conditions
- Part of the bbsink operation sequence: begin_archive → archive_contents → end_archive
- Forwards operation to next sink in chain for multi-destination backups
- Ensures data persistence before considering the archive file complete