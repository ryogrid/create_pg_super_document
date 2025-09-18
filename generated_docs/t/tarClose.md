# tarClose

## Location
[src/bin/pg_dump/pg_backup_tar.c:398-417](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_tar.c#L398-L417)

## Overview
Closes a TAR_MEMBER file handle and performs cleanup operations, adding the file to the tar archive if it was opened for writing.

## Definition
```c
static void tarClose(ArchiveHandle *AH, TAR_MEMBER *th)
```

## Detailed Description
The tarClose function is responsible for properly closing a TAR_MEMBER that was previously opened with tarOpen. For files opened in write mode, it calls _tarAddFile to add the temporary file's contents to the tar archive before cleaning up. For read mode, minimal cleanup is performed since no temporary files or duplicated file handles are used.

The function enforces the tar format's limitation of not supporting compression and will fail if compression is enabled. After closing, it frees the targetFile string and sets the file handle to NULL.

## Parameters / Member Variables
- `AH`: ArchiveHandle pointer containing archive context and format-specific data
- `th`: TAR_MEMBER pointer representing the file to be closed

## Dependencies
- Functions called/Symbols referenced:
  - [_tarAddFile](_tarAddFile.md)
  - free
- Called from (representative examples):
  - [_EndData](../E/_EndData.md)
  - [_PrintFileData](../P/_PrintFileData.md)
  - [_LoadLOs](../L/_LoadLOs.md)
  - [_CloseArchive](../C/_CloseArchive.md)
  - [_EndLO](../E/_EndLO.md)
  - [_EndLOs](../E/_EndLOs.md)

## Notes and Other Information
- Only performs significant work for write mode files by calling _tarAddFile
- For read mode files, only performs basic cleanup since no temporary files are involved
- Enforces that compression is not supported with tar format
- Frees memory allocated for targetFile string
- Sets file handle to NULL after closing for safety