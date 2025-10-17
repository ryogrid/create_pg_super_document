# open_walfile

## Location
[src/bin/pg_basebackup/receivelog.c:90-191](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/receivelog.c#L90-L191)

## Overview
Opens a new WAL file in the specified directory for writing during PostgreSQL base backup operations, with proper size validation and padding.

## Definition
```c
static bool open_walfile(StreamCtl *stream, XLogRecPtr startpoint)
```

## Detailed Description
This function creates and opens a new WAL (Write-Ahead Log) file for streaming during base backup operations. It handles existing file validation, ensures proper WAL segment sizing (16MB), and manages compression considerations. The function performs size checking for existing files to detect corruption, pads new files to the full WAL segment size, and handles different walmethod implementations (files vs tar archives).

## Parameters / Member Variables
- `stream`: Pointer to StreamCtl structure containing WAL streaming context, timeline, walmethod operations, and configuration
- `startpoint`: XLogRecPtr indicating the starting position for this WAL segment

## Dependencies
- Functions called/Symbols referenced:
  - [StreamCtl](../S/StreamCtl.md) (structure)
  - Walfile (structure)
  - XLogSegNo, XLogRecPtr (types)
  - XLByteToSeg
  - [XLogFileName](../X/XLogFileName.md)
  - PG_COMPRESSION_NONE
  - [GetLastWalMethodError](../G/GetLastWalMethodError.md)
  - CLOSE_UNLINK
  - [pg_free](../p/pg_free.md)
  - ngettext
  - walmethod->ops->get_file_name
  - walmethod->ops->existsfile
  - walmethod->ops->get_file_size
  - walmethod->ops->open_for_write
  - walmethod->ops->sync
  - walmethod->ops->close
- Called from (representative examples):
  - [ProcessXLogDataMsg](../P/ProcessXLogDataMsg.md)

## Notes and Other Information
- Pads WAL files to full 16MB (WalSegSz) with zeroes for uncompressed files
- Validates existing file sizes: must be 0 (empty) or WalSegSz (complete segment)
- Handles compression-aware file naming through walmethod operations
- Performs fsync on existing files to ensure durability after crashes
- Sets global `walfile` variable on successful opening
- Uses partial_suffix for temporary file naming during streaming
- Exits with error code 1 on critical fsync failures
- Different behavior for tar vs file-based walmethod implementations

## Simplified Source

```c
static bool
open_walfile(StreamCtl *stream, XLogRecPtr startpoint)
{
    Walfile    *f;
    char       *fn;
    ssize_t     size;
    XLogSegNo   segno;
    char        walfile_name[MAXPGPATH];

    // Generate WAL filename from startpoint
    XLByteToSeg(startpoint, segno, WalSegSz);
    XLogFileName(walfile_name, stream->timeline, segno, WalSegSz);

    // Get full filename including compression extension if needed
    fn = stream->walmethod->ops->get_file_name(stream->walmethod,
                                              walfile_name,
                                              stream->partial_suffix);

    // For uncompressed files, validate existing file size
    if (stream->walmethod->compression_algorithm == PG_COMPRESSION_NONE &&
        stream->walmethod->ops->existsfile(stream->walmethod, fn)) {

        size = stream->walmethod->ops->get_file_size(stream->walmethod, fn);
        if (size < 0) {
            pg_log_error("could not get size of WAL file \"%s\"", fn);
            pg_free(fn);
            return false;
        }

        if (size == WalSegSz) {
            // File already complete - open and sync it
            f = stream->walmethod->ops->open_for_write(stream->walmethod,
                                                      walfile_name,
                                                      stream->partial_suffix, 0);
            if (f && stream->walmethod->ops->sync(f) == 0) {
                walfile = f;
                pg_free(fn);
                return true;
            }
            // Handle sync/open errors...
            exit(1);
        }

        if (size != 0) {
            pg_log_error("WAL file \"%s\" has invalid size %zd, should be 0 or %d",
                        fn, size, WalSegSz);
            pg_free(fn);
            return false;
        }
    }

    // Create new WAL file with full segment size
    f = stream->walmethod->ops->open_for_write(stream->walmethod,
                                              walfile_name,
                                              stream->partial_suffix,
                                              WalSegSz);
    if (f == NULL) {
        pg_log_error("could not open WAL file \"%s\"", fn);
        pg_free(fn);
        return false;
    }

    pg_free(fn);
    walfile = f;
    return true;
}
```