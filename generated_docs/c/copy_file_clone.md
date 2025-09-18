# copy_file_clone

## Location
src/bin/pg_combinebackup/copy_file.c: 213 - 258

## Overview
Clones or reflinks a file from source to destination using platform-specific optimization techniques, with optional checksum calculation.

## Definition


## Detailed Description
The `copy_file_clone` function implements high-performance file copying using platform-specific cloning/reflink capabilities. On macOS, it uses the `copyfile` system call with `COPYFILE_CLONE_FORCE` flag. On Linux, it uses the `FICLONE` ioctl to create reflinks that share storage blocks until modified (copy-on-write). These techniques provide near-instantaneous copying for large files by creating metadata references rather than copying actual data blocks. If cloning fails, the function reports an error and cleans up partial files. After successful cloning, it separately calculates the checksum by reading the source file if needed.

## Parameters / Member Variables
- `src`: Path to the source file to clone from
- `dest`: Path to the destination file to create
- `checksum_ctx`: Pointer to checksum context for checksum calculation

## Dependencies  
- Functions called/Symbols referenced:
  - `copyfile` - macOS system call for file cloning (when available)
  - `open` - System call for file opening (Linux path)
  - `ioctl` - System call with FICLONE for Linux reflinks
  - `close` - System call for file closing
  - `unlink` - System call to remove failed destination file
  - `strerror` - Convert errno to error string
  - `[checksum_file](checksum_file.md)` - Calculate checksum of the cloned file
  - `pg_file_create_mode` - File creation permissions
- Called from:
  - `[copy_file](copy_file.md)` (src/bin/pg_combinebackup/copy_file.c:80) - as COPY_METHOD_CLONE strategy

## Notes and Other Information
- Platform-specific implementation with conditional compilation
- Provides dramatic performance improvements for large files through copy-on-write semantics
- Includes cleanup logic to remove partially created files on failure
- Checksum calculation is performed separately after cloning since data isn't read during clone
- Will fatal error on platforms that don't support file cloning
- Part of PostgreSQL's pg_combinebackup utility optimization strategies
- Location: src/bin/pg_combinebackup/copy_file.c:213-258