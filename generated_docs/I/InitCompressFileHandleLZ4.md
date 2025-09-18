# InitCompressFileHandleLZ4

## Location
src/bin/pg_dump/compress_lz4.c: 804 - 809

## Overview
Initializes a CompressFileHandle structure for LZ4 compressed file operations by setting up function pointers and allocating the necessary LZ4 state for stream-based compression and decompression.

## Definition


## Detailed Description
InitCompressFileHandleLZ4 is responsible for setting up the CompressFileHandle structure to work with LZ4-compressed files in PostgreSQL's pg_dump utility. The function has two different implementations based on compile-time LZ4 support:

**When LZ4 is enabled (USE_LZ4 defined):**
- Assigns LZ4-specific function pointers to the CompressFileHandle for all file operations (open, read, write, close, etc.)
- Allocates and initializes an LZ4State structure to maintain compression/decompression state
- Configures the compression level from the provided specification
- Sets up the private data pointer to reference the allocated LZ4 state

**When LZ4 is disabled (USE_LZ4 not defined):**
- Simply calls pg_fatal() to report that LZ4 compression is not supported in this build

The function creates a complete abstraction layer that allows the rest of the pg_dump code to work with LZ4 files using a consistent interface, regardless of whether the underlying operations involve compression or decompression.

## Parameters / Member Variables
- `CFH`: Pointer to the CompressFileHandle structure to be initialized with LZ4 functionality
- `compression_spec`: Specification containing compression parameters such as compression level

## Dependencies
- Functions called/Symbols referenced:
  - LZ4Stream_open (assigned as open_func)
  - LZ4Stream_open_write (assigned as open_write_func)
  - LZ4Stream_read (assigned as read_func)
  - LZ4Stream_write (assigned as write_func)
  - LZ4Stream_gets (assigned as gets_func)
  - LZ4Stream_getc (assigned as getc_func)
  - LZ4Stream_eof (assigned as eof_func)
  - LZ4Stream_close (assigned as close_func)
  - LZ4Stream_get_error (assigned as get_error_func)
  - pg_malloc0 (for state allocation)
- Called from (representative examples):
  - InitCompressFileHandle (in compress_io.c)

## Notes and Other Information
- This function is part of the public interface for LZ4 file handle management in pg_dump
- The function's behavior is conditional on compile-time LZ4 library availability
- Creates a function pointer abstraction layer that allows transparent LZ4 file operations
- The LZ4State is allocated but not fully initialized - specific initialization occurs during actual file operations
- The compression level configuration is stored in the LZ4State preferences for later use
- Error handling for unsupported builds uses PostgreSQL's standard pg_fatal() mechanism
- The function enables both reading from and writing to LZ4-compressed files through a unified interface
- Part of PostgreSQL's pluggable compression file handle architecture