# copy_file_by_range

## Location
src/bin/pg_combinebackup/copy_file.c: 259 - 293

## Overview
Copies a file from source to destination using the copy_file_range system call for efficient data transfer, with optional checksum calculation.

## Definition


## Detailed Description
The `copy_file_by_range` function utilizes the Linux `copy_file_range` system call to perform efficient file copying. This system call can optimize data transfer by avoiding unnecessary copies between kernel and user space, and may use advanced filesystem features like reflinks or server-side copy operations. The function repeatedly calls `copy_file_range` with `SSIZE_MAX` length until all data is copied, as the system call may not transfer the entire file in a single operation. After successful copying, it separately calculates the checksum by reading the source file if needed, since the copying doesn't provide access to the data stream for checksum computation.

## Parameters / Member Variables
- `src`: Path to the source file to copy from
- `dest`: Path to the destination file to create
- `checksum_ctx`: Pointer to checksum context for checksum calculation

## Dependencies
- Functions called/Symbols referenced:
  - `open` - System call for file opening
  - `copy_file_range` - Linux system call for efficient file copying
  - `close` - System call for file closing
  - `checksum_file` - Calculate checksum of the copied file
  - `pg_file_create_mode` - File creation permissions
  - `SSIZE_MAX` - Maximum value for ssize_t type
- Called from:
  - `copy_file` (src/bin/pg_combinebackup/copy_file.c:88) - as COPY_METHOD_COPY_FILE_RANGE strategy

## Notes and Other Information
- Linux-specific optimization requiring HAVE_COPY_FILE_RANGE compile-time support
- May provide performance benefits through kernel-level optimizations and reduced context switching
- Uses a loop since copy_file_range may not copy the entire file in one call
- Checksum calculation performed separately after copying since data doesn't pass through user space
- Will fatal error on platforms without copy_file_range support
- Part of PostgreSQL's pg_combinebackup utility advanced copying strategies
- Location: src/bin/pg_combinebackup/copy_file.c:259-293