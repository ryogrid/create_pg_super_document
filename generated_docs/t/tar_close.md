# tar_close

## Location
src/bin/pg_basebackup/walmethods.c: 1042 - 1218

## Overview
Closes a WAL file within a TAR archive, performing final operations including padding, header updates, and synchronization to persistent storage.

## Definition
```c
static int tar_close(Walfile *f, WalCloseMethod method)
```

## Detailed Description
This function implements the close operation for TAR-based WAL writing method in pg_basebackup. It's a complex function that handles multiple scenarios and performs several critical operations:

1. **Unlink Support**: When method is CLOSE_UNLINK, it truncates the TAR file to remove the just-written file entry (only supported for uncompressed files)

2. **Padding Management**: Handles padding to specified file sizes, with different approaches for compressed vs uncompressed files

3. **TAR Format Compliance**: Adds necessary padding to make file size a multiple of TAR_BLOCK_SIZE

4. **Header Updates**: Updates the TAR header with the final file size and recalculates the checksum

5. **Compression Handling**: Special logic for compressed TAR files, including flushing compressed data and temporarily disabling compression for header updates

6. **Synchronization**: Always performs fsync to ensure data is written to persistent storage

The function ensures TAR format compliance while supporting both compressed and uncompressed files.

## Parameters / Member Variables
- `f`: Pointer to the Walfile structure representing the open WAL file within the TAR method
- `method`: Enumeration value indicating how to close the file (CLOSE_NORMAL or CLOSE_UNLINK)

## Dependencies
- Functions called/Symbols referenced:
  - clear_error
  - ftruncate
  - pg_free
  - tar_write_padding_data
  - tarPaddingBytesRequired
  - tar_write
  - tar_write_compressed_data
  - print_tar_number
  - strlcpy
  - tarChecksum
  - lseek
  - write
  - deflateParams (zlib)
  - tar_sync
  - GetLastWalMethodError
  - pg_fatal
- Called from (representative examples):
  - CreateWalDirectoryMethod (function pointer assignment)
  - tar_finish

## Notes and Other Information
- This function is marked as static, meaning it's only accessible within the walmethods.c file
- Returns 0 on success, -1 on error with appropriate error information set
- CLOSE_UNLINK is only supported for uncompressed files
- The function performs complex header manipulation to update file size and checksum in the TAR archive
- For compressed files, compression parameters are temporarily modified to write the updated header
- Always calls tar_sync() to ensure data persistence, with a pg_fatal() call if sync fails
- Handles both padding requested at file creation time and TAR format-required padding
- Memory cleanup is performed for both the file pathname and the TarMethodFile structure
- The function seeks back to the end of the file after header updates to prepare for the next file