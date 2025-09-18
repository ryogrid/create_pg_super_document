# tar_open_for_write

## Location
src/bin/pg_basebackup/walmethods.c: 837 - 1006

## Overview
Opens a new file for writing within a TAR archive, handling TAR header creation, compression setup, and initial file positioning for PostgreSQL WAL operations.

## Definition
```c
static Walfile *tar_open_for_write(WalWriteMethod *wwmethod, const char *pathname, const char *temp_suffix, size_t pad_to_size)
```

## Detailed Description
This function is responsible for opening a new file within a TAR archive for writing. It performs several critical operations: opens the TAR file if not already open, initializes compression if gzip is enabled, ensures only one file is open at a time, creates TAR headers with proper metadata, handles compression parameter adjustments for headers vs content, and manages file padding for uncompressed files. The function integrates closely with PostgreSQL's WAL archiving system and supports both compressed and uncompressed TAR archives.

## Parameters / Member Variables
- `wwmethod`: Pointer to WalWriteMethod structure containing configuration and state
- `pathname`: Base pathname for the file to be created within the TAR archive
- `temp_suffix`: Optional temporary suffix for the filename (may be NULL)
- `pad_to_size`: Size to pad the file to (0 for no padding)

## Dependencies
- Functions called/Symbols referenced:
  - clear_error (error state reset)
  - open (system call for file opening)
  - tar_get_file_name (filename construction)
  - tarCreateHeader (TAR header creation)
  - tar_write_compressed_data (compression handling)
  - tar_write_padding_data (padding data writer)
  - deflateInit2, deflateParams (zlib compression functions)
  - lseek, write (file I/O system calls)
  - pg_malloc0, pg_malloc, pg_free, pg_strdup (PostgreSQL memory functions)
  - Various constants: PG_COMPRESSION_GZIP, PG_COMPRESSION_NONE, TAR_BLOCK_SIZE, etc.
- Called from:
  - CreateWalDirectoryMethod (as function pointer assignment)

## Notes and Other Information
- Returns pointer to Walfile structure on success, NULL on failure
- Enforces single-file-at-a-time constraint for TAR archives
- Lazy-opens the TAR file only when first needed
- For gzip compression, temporarily disables compression for TAR headers
- Handles file padding differently for compressed vs uncompressed files
- Sets file permissions to S_IRUSR | S_IWUSR (user read/write only)
- Maintains current file position tracking and start offset information
- Critical component of PostgreSQL's WAL archiving infrastructure supporting both streaming and file-based backup operations