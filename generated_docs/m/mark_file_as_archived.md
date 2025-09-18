# mark_file_as_archived

## Location
src/bin/pg_basebackup/receivelog.c: 54 - 89

## Overview
Creates an archive status file marking a WAL file as successfully archived during PostgreSQL base backup operations.

## Definition
```c
static bool mark_file_as_archived(StreamCtl *stream, const char *fname)
```

## Detailed Description
This function creates a `.done` file in the `archive_status` directory to indicate that a WAL file has been successfully archived. This is part of PostgreSQL's WAL archiving mechanism used during base backup operations. The function uses the walmethod operations to create and close the status file, ensuring proper error handling throughout the process.

## Parameters / Member Variables
- `stream`: Pointer to StreamCtl structure containing WAL streaming context and walmethod operations
- `fname`: Name of the WAL file that has been archived (without path)

## Dependencies
- Functions called/Symbols referenced:
  - [StreamCtl](../S/StreamCtl.md) (structure)
  - Walfile (structure) 
  - [GetLastWalMethodError](../G/GetLastWalMethodError.md)
  - CLOSE_NORMAL
  - walmethod->ops->open_for_write
  - walmethod->ops->close
- Called from (representative examples):
  - [close_walfile](../c/close_walfile.md)
  - [writeTimeLineHistoryFile](../w/writeTimeLineHistoryFile.md)

## Notes and Other Information
- Creates archive status files with `.done` extension in the `archive_status/` subdirectory
- Uses static buffer for temporary path construction (MAXPGPATH size)
- Returns false on any error during file creation or closing
- Error messages are logged using pg_log_error for debugging purposes
- Part of the WAL archiving status tracking system in pg_basebackup utility