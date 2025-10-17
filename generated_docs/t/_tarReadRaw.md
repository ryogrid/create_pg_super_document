# _tarReadRaw

## Location
[src/bin/pg_dump/pg_backup_tar.c:462-510](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_tar.c#L462-L510)

## Overview
The low-level read routine that handles all read operations on tar archive files, with support for lookahead buffering and flexible file handle sources.

## Definition
```c
static size_t _tarReadRaw(ArchiveHandle *AH, void *buf, size_t len, TAR_MEMBER *th, FILE *fh)
```

## Detailed Description
_tarReadRaw is the fundamental read operation for the tar archive format implementation. It serves as the unified interface for all read operations within the tar handling system. The function implements a lookahead buffer mechanism to optimize read performance and handle cases where small amounts of data need to be examined ahead of the current position.

The function can read from either a TAR_MEMBER file handle or a direct FILE pointer, providing flexibility for different reading contexts. It first attempts to satisfy the read request using any available lookahead data, then performs actual file I/O operations as needed. The function maintains position tracking for the tar file and handles read errors appropriately.

## Parameters / Member Variables
- `AH`: ArchiveHandle pointer containing lookahead buffer and archive context
- `buf`: Buffer to store the read data
- `len`: Number of bytes to read
- `th`: TAR_MEMBER pointer for member-specific reads (can be NULL if fh is provided)
- `fh`: FILE pointer for direct file reads (can be NULL if th is provided)

## Dependencies
- Functions called/Symbols referenced:
  - memcpy
  - fread
  - feof
  - READ_ERROR_EXIT
- Called from (representative examples):
  - [tarGets](tarGets.md)
  - [tarRead](tarRead.md)
  - [_tarPositionTo](_tarPositionTo.md)
  - [_tarGetHeader](_tarGetHeader.md)

## Notes and Other Information
- Either th or fh must be provided (enforced by Assert)
- Implements lookahead buffering for performance optimization
- Handles partial reads from lookahead buffer efficiently
- Updates tar file position tracking (tarFHpos) after each read
- Uses READ_ERROR_EXIT macro for consistent error handling
- Returns actual number of bytes read, which may be less than requested at EOF
- Critical foundation function used by all higher-level tar reading operations

## Simplified Source

```c
static size_t _tarReadRaw(ArchiveHandle *AH, void *buf, size_t len, TAR_MEMBER *th, FILE *fh) {
    lclContext *ctx = (lclContext *) AH->formatData;
    size_t used = 0;
    size_t res = 0;

    // Use lookahead buffer if available
    size_t avail = AH->lookaheadLen - AH->lookaheadPos;
    if (avail > 0) {
        used = (avail >= len) ? len : avail;
        memcpy(buf, AH->lookahead + AH->lookaheadPos, used);
        AH->lookaheadPos += used;
        len -= used;
    }

    // Read remaining data from file if needed
    if (len > 0) {
        FILE *file_handle = fh ? fh : th->nFH;
        res = fread(&((char *) buf)[used], 1, len, file_handle);
        if (res != len && !feof(file_handle))
            READ_ERROR_EXIT(file_handle);
    }

    // Update position and return total bytes read
    ctx->tarFHpos += res + used;
    return res + used;
}
```