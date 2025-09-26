# tar_finish

## Location
src/bin/pg_basebackup/walmethods.c: 1227 - 1335

## Overview
Finalizes a TAR-based WAL method by closing any open files, writing TAR termination blocks, and ensuring all data is synchronized to persistent storage.

## Definition
```c
static bool tar_finish(WalWriteMethod *wwmethod)
```

## Detailed Description
This function completes the TAR-based WAL writing process in pg_basebackup. It performs several critical finalization steps:

1. **Close Open Files**: If there's a currently open file in the TAR, it closes it using tar_close() with CLOSE_NORMAL method

2. **TAR Format Compliance**: Writes the required two empty 512-byte blocks at the end of the TAR file to mark the end of the archive

3. **Compression Finalization**: For compressed TAR files, it properly finalizes the compression stream using deflate() with Z_FINISH and deflateEnd()

4. **Data Synchronization**: Performs multiple levels of fsync operations to ensure data persistence:
   - Syncs the file descriptor
   - Syncs the filename 
   - Syncs the parent directory

5. **Resource Cleanup**: Closes the TAR file descriptor and marks it as closed

The function handles both compressed and uncompressed TAR files appropriately, ensuring proper format compliance and data durability.

## Parameters / Member Variables
- `wwmethod`: Pointer to the WalWriteMethod structure representing the TAR-based WAL writing method to finalize

## Dependencies
- Functions called/Symbols referenced:
  - clear_error
  - tar_close
  - write
  - tar_write_compressed_data
  - deflate (zlib)
  - deflateEnd (zlib)
  - fsync
  - close
  - fsync_fname
  - fsync_parent_path
  - TarMethodData (structure type)
  - CLOSE_NORMAL (constant)
  - PG_COMPRESSION_NONE (constant)
  - PG_COMPRESSION_GZIP (constant)
  - ZLIB_OUT_SIZE (constant)
- Called from (representative examples):
  - CreateWalDirectoryMethod (function pointer assignment)

## Notes and Other Information
- This function is marked as static, meaning it's only accessible within the walmethods.c file
- Returns true on success, false on error with appropriate error information set
- Writes exactly 1024 bytes of zero data to terminate the TAR file (two 512-byte blocks)
- For compressed files, performs a complete deflate finalization loop to ensure all compressed data is written
- Implements comprehensive synchronization strategy including file, filename, and parent directory sync
- Sets the file descriptor to -1 after closing to indicate the TAR file is no longer open
- Uses errno checking with fallback to ENOSPC when write operations don't set errno
- The function ensures TAR format compliance by properly terminating the archive structure