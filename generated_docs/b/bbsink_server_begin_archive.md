# bbsink_server_begin_archive

## Location
[src/backend/backup/basebackup_server.c:134-159](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_server.c#L134-L159)

## Overview
Opens a new output file on the server filesystem for storing a backup archive with the specified name.

## Definition


## Detailed Description
This function initiates a new archive file for a basebackup operation on the server filesystem. It constructs the full file path by combining the sink's configured pathname with the provided archive name, then opens the file with exclusive creation flags to prevent overwrites. The function ensures the file is opened in binary mode for cross-platform compatibility and forwards the operation to the next sink in the chain.

The function includes safety assertions to verify that no file is currently open and the file position is at zero, ensuring clean state between archive operations.

## Parameters / Member Variables
- : Pointer to the bbsink instance (cast to bbsink_server internally)
- : Name of the archive file to create (e.g., "base.tar", "pg_wal.tar")

## Dependencies
- Functions called/Symbols referenced:
  - [psprintf](../p/psprintf.md)
  - PathNameOpenFile
  - [pfree](../p/pfree.md)
  - ereport
  - [errcode_for_file_access](../e/errcode_for_file_access.md)
  - [errmsg](../e/errmsg.md)
  - [bbsink_forward_begin_archive](bbsink_forward_begin_archive.md)
- Called from (representative examples):
  - Referenced through bbsink_server_ops function table

## Notes and Other Information
- Uses O_CREAT | O_EXCL flags to ensure files are created exclusively (fails if file exists)
- Opens files in binary mode (PG_BINARY) for platform independence
- File handle stored in mysink->file for subsequent operations
- Automatically forwards operation to next sink in chain for multi-destination backups
- Part of the bbsink operation sequence: begin_archive → archive_contents → end_archive