# mark_file_as_archived

## Location
[src/bin/pg_basebackup/receivelog.c:54-89](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/receivelog.c#L54-L89)

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

## Simplified Source

```c
static bool
mark_file_as_archived(StreamCtl *stream, const char *fname)
{
    static char tmppath[MAXPGPATH];
    Walfile *f;

    // Create path for .done status file
    snprintf(tmppath, sizeof(tmppath), "archive_status/%s.done", fname);

    // Create the status file
    f = stream->walmethod->ops->open_for_write(stream->walmethod, tmppath, NULL, 0);
    if (f == NULL) {
        pg_log_error("could not create archive status file \"%s\": %s",
                     tmppath, GetLastWalMethodError(stream->walmethod));
        return false;
    }

    // Close the status file
    if (stream->walmethod->ops->close(f, CLOSE_NORMAL) != 0) {
        pg_log_error("could not close archive status file \"%s\": %s",
                     tmppath, GetLastWalMethodError(stream->walmethod));
        return false;
    }

    return true;
}
```