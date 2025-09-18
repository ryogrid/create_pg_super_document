# bbsink_server_archive_contents

## Location
src/backend/backup/basebackup_server.c: 160 - 193

## Overview
Writes backup archive data content to the currently opened server-side output file with comprehensive error handling.

## Definition


## Detailed Description
This function writes a chunk of backup archive data to the server filesystem. It performs the actual file I/O operation using PostgreSQL's FileWrite interface, which provides proper error handling and wait event tracking. The function handles both partial writes and complete write failures, providing detailed error messages to help diagnose storage issues.

The function maintains the current file position and provides comprehensive error reporting including disk space hints when write operations fail. After successful writing, it forwards the operation to the next sink in the chain.

## Parameters / Member Variables
- : Pointer to the bbsink instance (cast to bbsink_server internally)  
- : Number of bytes to write from the sink's buffer (bbs_buffer)

## Dependencies
- Functions called/Symbols referenced:
  - [FileWrite](../F/FileWrite.md)
  - [FilePathName](../F/FilePathName.md)  
  - ereport
  - [errcode_for_file_access](../e/errcode_for_file_access.md)
  - [errcode](../e/errcode.md) (ERRCODE_DISK_FULL)
  - [errmsg](../e/errmsg.md)
  - [errhint](../e/errhint.md)
  - [bbsink_forward_archive_contents](bbsink_forward_archive_contents.md)
- Called from (representative examples):
  - Referenced through bbsink_server_ops function table

## Notes and Other Information
- Writes data from mysink->base.bbs_buffer at the current file position
- Updates mysink->filepos to track current position in the file
- Provides specific error handling for both I/O errors and disk full conditions
- Uses WAIT_EVENT_BASEBACKUP_WRITE for proper wait event monitoring
- Includes helpful hints about checking disk space when write failures occur
- Part of the bbsink operation sequence: begin_archive → archive_contents → end_archive
- Can be called multiple times during an archive to write chunks of data