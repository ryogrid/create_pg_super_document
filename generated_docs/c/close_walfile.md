# close_walfile

## Location
[src/bin/pg_basebackup/receivelog.c:192-257](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/receivelog.c#L192-L257)

## Overview
Closes the currently open WAL file, handles renaming for complete segments, and optionally marks files as archived during PostgreSQL base backup operations.

## Definition
```c
static bool close_walfile(StreamCtl *stream, XLogRecPtr pos)
```

## Detailed Description
This function properly closes the current WAL file that is being written during streaming operations. It handles different closing modes based on whether the WAL segment is complete (full WalSegSz), manages file renaming through the walmethod operations, and optionally creates archive status files to mark segments as archived. The function ensures proper cleanup of the global walfile pointer and updates the last flush position.

## Parameters / Member Variables
- `stream`: Pointer to StreamCtl structure containing WAL streaming context, walmethod operations, and archiving configuration
- `pos`: XLogRecPtr indicating the current position for updating lastFlushPosition

## Dependencies
- Functions called/Symbols referenced:
  - [StreamCtl](../S/StreamCtl.md) (structure)
  - pgoff_t (type)
  - [strlcpy](../s/strlcpy.md)
  - CLOSE_NORMAL, CLOSE_NO_RENAME (constants)
  - walmethod->ops->get_file_name
  - walmethod->ops->close
  - pg_log_info
  - pg_log_error
  - [GetLastWalMethodError](../G/GetLastWalMethodError.md)
  - [pg_free](../p/pg_free.md)
  - [mark_file_as_archived](../m/mark_file_as_archived.md)
- Called from (representative examples):
  - [ProcessXLogDataMsg](../P/ProcessXLogDataMsg.md)
  - [HandleEndOfCopyStream](../H/HandleEndOfCopyStream.md)
  - [CheckCopyStreamStop](../C/CheckCopyStreamStop.md)

## Notes and Other Information
- Sets global `walfile` variable to NULL after closing
- Uses CLOSE_NORMAL for complete segments, CLOSE_NO_RENAME for incomplete ones
- Only renames files when using partial_suffix and segment is complete (currpos == WalSegSz)
- Conditionally marks files as archived when stream->mark_done is set and segment is complete
- Updates global lastFlushPosition variable on successful completion
- Handles compression-aware file naming through walmethod operations
- Returns false on any error during closing or archiving operations
- Prevents duplicate archiving in pg_basebackup scenarios by marking segments as done

## Simplified Source

```c
static bool
close_walfile(StreamCtl *stream, XLogRecPtr pos)
{
    char       *fn;
    pgoff_t     currpos;
    int         r;
    char        walfile_name[MAXPGPATH];

    // Nothing to close if no file is open
    if (walfile == NULL)
        return true;

    // Save current file info before closing
    strlcpy(walfile_name, walfile->pathname, MAXPGPATH);
    currpos = walfile->currpos;

    // Get filename with appropriate suffix/compression
    fn = stream->walmethod->ops->get_file_name(stream->walmethod,
                                              walfile_name,
                                              stream->partial_suffix);

    // Choose close mode based on completion status
    if (stream->partial_suffix) {
        if (currpos == WalSegSz)
            r = stream->walmethod->ops->close(walfile, CLOSE_NORMAL);
        else {
            pg_log_info("not renaming \"%s\", segment is not complete", fn);
            r = stream->walmethod->ops->close(walfile, CLOSE_NO_RENAME);
        }
    } else
        r = stream->walmethod->ops->close(walfile, CLOSE_NORMAL);

    walfile = NULL;

    if (r != 0) {
        pg_log_error("could not close file \"%s\": %s",
                    fn, GetLastWalMethodError(stream->walmethod));
        pg_free(fn);
        return false;
    }

    pg_free(fn);

    // Mark complete segments as archived if requested
    if (currpos == WalSegSz && stream->mark_done) {
        if (!mark_file_as_archived(stream, walfile_name))
            return false;
    }

    lastFlushPosition = pos;
    return true;
}
```