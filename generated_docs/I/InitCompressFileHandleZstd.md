# InitCompressFileHandleZstd

## Location
src/bin/pg_dump/compress_zstd.c: 559 - 577

## Overview
Initializes a CompressFileHandle structure with zstd-specific function pointers and compression settings for use in PostgreSQL's pg_dump utility.

## Definition


## Detailed Description
This function serves as the initialization routine for the zstd compression backend in pg_dump. It configures a CompressFileHandle by assigning zstd-specific implementations to all the function pointers required for file I/O operations. The function sets up the complete interface for reading, writing, opening, closing, and error handling with zstd-compressed files. It also stores the compression specification and initializes the private data pointer to NULL, which will later hold the ZstdCompressorState when compression operations begin.

## Parameters / Member Variables
- `CFH`: Pointer to the CompressFileHandle structure to be initialized with zstd functionality
- `compression_spec`: Structure containing compression-specific configuration parameters (level, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - CompressFileHandle (structure)
  - pg_compress_specification (structure)  
  - Zstd_open (function pointer assignment)
  - Zstd_open_write (function pointer assignment)
  - Zstd_read (function pointer assignment)
  - Zstd_write (function pointer assignment)
  - Zstd_gets (function pointer assignment)
  - Zstd_getc (function pointer assignment)
  - Zstd_close (function pointer assignment)
  - Zstd_eof (function pointer assignment)
  - Zstd_get_error (function pointer assignment)
- Called from (representative examples):
  - InitCompressFileHandle (main compression backend dispatcher)

## Notes and Other Information
- This is a public function (not static) that serves as the entry point for zstd compression
- Creates a complete function table for zstd operations through the generic compression interface
- The private_data field is initialized to NULL and allocated later during actual compression operations
- Part of the modular compression system that allows pg_dump to support multiple compression formats
- The compression_spec parameter allows configuration of zstd-specific settings like compression level