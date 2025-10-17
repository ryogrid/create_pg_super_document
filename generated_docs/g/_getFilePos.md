# _getFilePos

## Location
[src/bin/pg_dump/pg_backup_custom.c:936-955](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_custom.c#L936-L955)

## Overview
This function retrieves the current file position within a custom-format archive file, handling both seekable and non-seekable archives gracefully.

## Definition
```c
static pgoff_t _getFilePos(ArchiveHandle *AH, lclContext *ctx)
```

## Detailed Description
_getFilePos obtains the current position in the archive file using the standard ftello() function. It is designed to handle both seekable and non-seekable archive files. For non-seekable files, the function returns -1 to indicate that the position cannot be determined, which is acceptable since TOC data block offsets cannot be rewritten in such cases anyway. If the file was previously determined to be seekable (ctx->hasSeek is true) but ftello() fails, the function treats this as a fatal error since it indicates an unexpected condition.

## Parameters / Member Variables
- `AH`: Archive handle containing the file handle and other archive state information
- `ctx`: Local context structure containing seek capability information and other format-specific state

## Dependencies
- Functions called/Symbols referenced:
  - [lclContext](../l/lclContext.md) (structure type)
  - pgoff_t (type for file offsets)
  - ftello (standard library function for getting file position)
  - [pg_fatal](../p/pg_fatal.md) (PostgreSQL error reporting function)
- Called from (representative examples):
  - [_StartData](../S/_StartData.md) (to record data block positions)
  - [_StartLOs](../S/_StartLOs.md) (to record large object positions)
  - [_PrintTocData](../P/_PrintTocData.md) (to track current position during TOC operations)

## Notes and Other Information
- This is a static function internal to pg_backup_custom.c
- Returns -1 for non-seekable files, which is handled gracefully by callers
- The hasSeek flag in the context determines whether seek failures should be fatal
- Used for recording file positions in TOC entries to enable efficient random access during restore
- Part of the custom archive format's position tracking mechanism
- The returned position is used to populate the dataPos field in lclTocEntry structures

## Simplified Source

```c
static pgoff_t
_getFilePos(ArchiveHandle *AH, lclContext *ctx)
{
    pgoff_t position;

    // Get current file position
    position = ftello(AH->FH);

    if (position < 0) {
        // If file was supposed to be seekable, this is unexpected
        if (ctx->hasSeek) {
            pg_fatal("could not determine seek position in archive file: %m");
        }
        // For non-seekable files, return -1 (acceptable limitation)
    }

    return position;
}
```