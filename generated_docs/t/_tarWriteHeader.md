# _tarWriteHeader

## Location
[src/bin/pg_dump/pg_backup_tar.c:1212-1222](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_tar.c#L1212-L1222)

## Overview
Creates and writes a TAR format header block during PostgreSQL base backup operations, formatting file metadata according to POSIX TAR specifications.

## Definition
```c
static int64 _tarWriteHeader(bbsink *sink, const char *filename, const char *linktarget, struct stat *statbuf, bool sizeonly)
```

## Detailed Description
This function is a critical component of PostgreSQL's streaming base backup functionality, responsible for generating TAR format headers that precede file data in backup streams. It creates properly formatted TAR headers containing file metadata (name, size, permissions, timestamps, etc.) and writes them to the backup sink.

The function performs several key operations:
- Validates buffer size constraints to ensure TAR blocks fit within PostgreSQL's block size requirements
- Calls tarCreateHeader to format the TAR header with file metadata from stat information
- Handles various TAR format limitations (filename length, symlink target length)
- Provides comprehensive error reporting for TAR format violations
- Manages the backup sink buffering and content archival process

When sizeonly is true, the function performs size calculation without actually writing header data, useful for backup planning and progress reporting.

## Parameters / Member Variables
- `sink`: Base backup sink object that manages the output stream and buffering for the backup data
- `filename`: Path/name of the file being backed up, must comply with TAR format filename length limits
- `linktarget`: Target path for symbolic links, NULL for regular files, must comply with TAR symlink length limits  
- `statbuf`: File system stat structure containing file metadata (size, permissions, timestamps, ownership)
- `sizeonly`: Boolean flag indicating whether to only calculate size (true) or actually write the header (false)

## Dependencies
- Functions called/Symbols referenced:
  - tarCreateHeader
  - [bbsink_archive_contents](../b/bbsink_archive_contents.md)
  - StaticAssertDecl
  - ereport
  - elog
- Called from (representative examples):
  - [sendFileWithContent](../s/sendFileWithContent.md)
  - [sendDir](../s/sendDir.md)
  - [sendFile](../s/sendFile.md)
  - [_tarAddFile](_tarAddFile.md)

## Notes and Other Information
- Returns TAR_BLOCK_SIZE (512 bytes) representing the size of the written header
- Implements compile-time assertions to ensure buffer size compatibility with TAR block requirements
- Provides detailed error handling for TAR format violations including filename and symlink length limits
- Uses PostgreSQL's error reporting system (ereport/elog) for consistent error messaging
- The function is designed to work with PostgreSQL's streaming backup architecture
- Buffer management is handled through the bbsink interface for efficient I/O operations
- Supports both regular file headers and symbolic link headers through the linktarget parameter

## Simplified Source

```c
// Simplified version of _tarWriteHeader
static int64 _tarWriteHeader(bbsink *sink, const char *filename, const char *linktarget,
                            struct stat *statbuf, bool sizeonly) {
    enum tarError rc;

    if (!sizeonly) {
        // Validate buffer size for TAR block
        StaticAssertDecl(TAR_BLOCK_SIZE <= BLCKSZ, "BLCKSZ too small for tar block");
        Assert(sink->bbs_buffer_length >= TAR_BLOCK_SIZE);

        // Create TAR header in buffer
        rc = tarCreateHeader(sink->bbs_buffer, filename, linktarget,
                            statbuf->st_size, statbuf->st_mode,
                            statbuf->st_uid, statbuf->st_gid,
                            statbuf->st_mtime);

        // Handle TAR format errors
        switch (rc) {
            case TAR_OK:
                break;
            case TAR_NAME_TOO_LONG:
                ereport(ERROR,
                        (errmsg("file name too long for tar format: \"%s\"", filename)));
                break;
            case TAR_SYMLINK_TOO_LONG:
                ereport(ERROR,
                        (errmsg("symbolic link target too long for tar format: "
                                "file name \"%s\", target \"%s\"",
                                filename, linktarget)));
                break;
            default:
                elog(ERROR, "unrecognized tar error: %d", rc);
        }

        // Send header to backup sink
        bbsink_archive_contents(sink, TAR_BLOCK_SIZE);
    }

    return TAR_BLOCK_SIZE;
}
```

Key simplifications made:
- Removed detailed comments while preserving essential logic
- Streamlined the error handling switch statement
- Maintained TAR format validation and buffer size checks
- Preserved the essential TAR header creation and transmission functionality
- Kept all error reporting for format violations