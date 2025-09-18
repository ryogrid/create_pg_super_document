# Gzip_open

## Location
src/bin/pg_dump/compress_gzip.c: 359 - 388

## Overview
Opens a gzip-compressed file for reading or writing operations through PostgreSQL's compression abstraction interface.

## Definition
static bool Gzip_open(const char *path, int fd, const char *mode, CompressFileHandle *CFH)

## Detailed Description
This function opens a gzip-compressed file using either a file path or a file descriptor. It supports custom compression levels specified in the CompressFileHandle structure and creates the appropriate mode string for zlib operations.

The function first checks if a custom compression level has been specified in CFH->compression_spec.level. If a level other than Z_DEFAULT_COMPRESSION is set, it appends the compression level digit to the mode string (e.g., "w6" for write mode with compression level 6). If no custom level is specified, it uses the mode string as-is.

The function supports two ways of opening files:
1. If a valid file descriptor (fd >= 0) is provided, it uses gzdopen() with a duplicated file descriptor
2. Otherwise, it uses gzopen() with the file path

Upon successful opening, the gzFile handle is stored in the CompressFileHandle's private_data field for use by other gzip functions.

## Parameters / Member Variables
- `path`: File path to open (used when fd < 0)
- `fd`: File descriptor to use (takes precedence over path if >= 0)  
- `mode`: File access mode (e.g., "r", "w", "a")
- `CFH`: Pointer to CompressFileHandle structure to initialize

## Dependencies
- Functions called/Symbols referenced:
  - gzdopen (from zlib library)
  - gzopen (from zlib library)
  - dup (system call for duplicating file descriptors)
  - snprintf (standard C library function)
  - strcpy (standard C library function)
  - Z_DEFAULT_COMPRESSION (zlib constant)
  - CompressFileHandle (structure type)
- Called from (representative examples):
  - No direct references found in the codebase (likely called through function pointers in compression interface)

## Notes and Other Information
- Returns true on success, false if the file could not be opened
- Supports custom compression levels from 0 (no compression) to 9 (maximum compression)
- Uses dup() to duplicate file descriptors to avoid conflicts with caller's file handling
- This is a static function, so it's only accessible within the compress_gzip.c file
- Part of the gzip compression backend for PostgreSQL's pg_dump utility
- Requires HAVE_LIBZ to be defined for compilation (depends on zlib library)
- Essential for initializing gzip file handles for both reading and writing compressed data