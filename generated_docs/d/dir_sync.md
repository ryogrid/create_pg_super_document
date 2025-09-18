# dir_sync

## Location
[src/bin/pg_basebackup/walmethods.c:514-564](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/walmethods.c#L514-L564)

## Overview
Synchronizes a WAL file to persistent storage, handling compression-specific flushing operations and performing fsync on the underlying file descriptor.

## Definition
```c
static int
dir_sync(Walfile *f)
```

## Detailed Description
This function ensures that data written to a WAL file is synchronized to persistent storage. It handles synchronization differently based on the compression algorithm being used:

1. **No synchronization mode**: If sync is disabled in wwmethod, returns immediately without performing any operations
2. **Gzip compression**: Uses gzflush() with Z_SYNC_FLUSH to flush internal compression buffers
3. **LZ4 compression**: Uses LZ4F_flush() to flush internal buffers, then writes any remaining compressed data to the file descriptor
4. **All cases**: Calls fsync() on the underlying file descriptor to ensure data reaches persistent storage

The function is critical for ensuring data durability in synchronous WAL writing modes. For compressed files, it first ensures that compression libraries have flushed their internal buffers, then synchronizes the underlying file descriptor.

## Parameters / Member Variables
- `f`: Pointer to Walfile structure representing the file to synchronize

## Dependencies
- Functions called/Symbols referenced:
  - clear_error (error state initialization)
  - gzflush (gzip buffer flushing)
  - LZ4F_flush, LZ4F_isError, LZ4F_getErrorName (LZ4 compression operations)
  - write (system call for LZ4 flushed data)
  - fsync (file synchronization system call)
- Called from (representative examples):
  - WAL synchronization operations in pg_basebackup

## Notes and Other Information
- This is a static function, only accessible within the walmethods.c file
- Returns 0 on success, -1 on error
- Short-circuits if synchronization is disabled in the write method configuration
- For gzip files, gzflush() does not guarantee fsync, so explicit fsync() is still required
- For LZ4 files, flushed data must be written to the file descriptor before fsync
- The function only performs operations when sync mode is enabled
- Critical for data durability in synchronous backup operations
- Error details are stored in wwmethod->lasterrno or lasterrstring as appropriate