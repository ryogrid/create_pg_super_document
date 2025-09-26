# bbsink_server_begin_manifest

## Location
[src/backend/backup/basebackup_server.c:228-252](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_server.c#L228-L252)

## Overview
Opens a temporary backup manifest file on the server filesystem, preparing to write the backup manifest that will be renamed to its final location upon completion.

## Definition

```c
static void
bbsink_server_begin_manifest(bbsink *sink)
```
## Detailed Description
This function initiates the backup manifest writing process by creating a temporary manifest file. It follows the same atomic file creation pattern as pg_basebackup, where the manifest is first written to a temporary file (backup_manifest.tmp) and only renamed to its final name (backup_manifest) after successful completion and fsync. This approach ensures that the presence of a properly named manifest file guarantees the backup completed successfully.

The function creates the temporary file with exclusive creation flags to prevent conflicts and opens it in binary mode for cross-platform compatibility.

## Parameters / Member Variables
- : Pointer to the bbsink instance (cast to bbsink_server internally)

## Dependencies
- Functions called/Symbols referenced:
  - [psprintf](../p/psprintf.md)
  - [PathNameOpenFile](../P/PathNameOpenFile.md)
  - [pfree](../p/pfree.md)
  - ereport
  - [errcode_for_file_access](../e/errcode_for_file_access.md)
  - [errmsg](../e/errmsg.md)
  - [bbsink_forward_begin_manifest](bbsink_forward_begin_manifest.md)
- Called from (representative examples):
  - Referenced through bbsink_server_ops function table

## Notes and Other Information
- Creates backup_manifest.tmp as temporary file in the backup directory
- Uses O_CREAT | O_EXCL flags to ensure exclusive creation (fails if file exists)
- Opens file in binary mode (PG_BINARY) for platform independence
- Follows atomic file creation pattern: write to temporary, then rename to final
- File handle stored in mysink->file for subsequent manifest writing operations
- Part of the bbsink manifest sequence: begin_manifest → manifest_contents → end_manifest
- The final manifest file serves as a completion indicator for the entire backup
- Automatically forwards operation to next sink in chain for multi-destination backups