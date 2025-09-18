# dir_close

## Location
src/bin/pg_basebackup/walmethods.c: 385 - 513

## Overview
Closes a WAL file in the directory-based method, handling compression finalization, file operations (rename/unlink), and resource cleanup based on the specified close method.

## Definition
```c
static int
dir_close(Walfile *f, WalCloseMethod method)
```

## Detailed Description
This function handles closing WAL files in the directory-based method, performing different operations based on the compression algorithm and close method specified. The function manages several critical tasks:

1. **Compression finalization**: For LZ4 compression, writes final compressed data and frees compression context. For gzip, calls gzclose().
2. **File operations**: Depending on the close method:
   - CLOSE_NORMAL with temp_suffix: Renames temporary file to permanent name
   - CLOSE_UNLINK: Deletes the file
   - CLOSE_NORMAL without temp_suffix or CLOSE_NO_RENAME: Syncs file if sync mode enabled
3. **Resource cleanup**: Frees all allocated memory and closes file descriptors
4. **Synchronization**: Performs fsync operations when sync mode is enabled

The function handles three close methods:
- **CLOSE_NORMAL**: Normal file closure, with optional rename if temp_suffix exists
- **CLOSE_UNLINK**: Closes and deletes the file
- **CLOSE_NO_RENAME**: Closes file without renaming, syncing if requested

## Parameters / Member Variables
- `f`: Pointer to Walfile structure representing the file to close
- `method`: WalCloseMethod specifying how to close the file (CLOSE_NORMAL, CLOSE_UNLINK, CLOSE_NO_RENAME)

## Dependencies
- Functions called/Symbols referenced:
  - clear_error (error state initialization)
  - gzclose (gzip file closure)
  - LZ4F_compressEnd, LZ4F_freeCompressionContext (LZ4 compression cleanup)
  - write, close (system calls)
  - [dir_get_file_name](dir_get_file_name.md) (filename construction)
  - rename, durable_rename (file renaming)
  - unlink (file deletion)
  - [fsync_fname](../f/fsync_fname.md), fsync_parent_path (synchronization)
  - [pg_free](../p/pg_free.md) (memory deallocation)
- Called from (representative examples):
  - WAL file management in pg_basebackup

## Notes and Other Information
- This is a static function, only accessible within the walmethods.c file
- Returns 0 on success, -1 on error
- For LZ4 compression, writes final compressed chunk before closing
- Handles both durable and non-durable rename operations based on sync settings
- Performs complete resource cleanup including freeing compression buffers and contexts
- The function sets lasterrno in the wwmethod structure on errors
- File synchronization is performed when sync mode is enabled and appropriate for the close method
- Memory allocated for pathname, fullpath, and temp_suffix is properly freed