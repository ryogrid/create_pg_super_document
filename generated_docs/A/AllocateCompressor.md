# AllocateCompressor

## Location
src/bin/pg_dump/compress_io.c: 124 - 148

## Overview
This function allocates and initializes a new CompressorState structure for handling compression operations based on the specified compression algorithm.

## Definition


## Detailed Description
The `AllocateCompressor` function creates a new compressor instance by allocating memory for a CompressorState structure and initializing it with the appropriate compression algorithm. The function serves as a factory method that delegates the specific initialization to algorithm-specific functions based on the compression specification provided. It sets up the read and write function pointers that will be used for I/O operations during compression.

The function supports multiple compression algorithms:
- PG_COMPRESSION_NONE: No compression (pass-through)
- PG_COMPRESSION_GZIP: GNU zip compression using zlib
- PG_COMPRESSION_LZ4: LZ4 fast compression
- PG_COMPRESSION_ZSTD: Zstandard compression

## Parameters / Member Variables
- `compression_spec`: A pg_compress_specification structure specifying the compression algorithm and its parameters
- `readF`: Function pointer for reading data (ReadFunc type)
- `writeF`: Function pointer for writing data (WriteFunc type)

## Dependencies
- Functions called/Symbols referenced:
  - pg_malloc0
  - [InitCompressorNone](../I/InitCompressorNone.md)
  - [InitCompressorGzip](../I/InitCompressorGzip.md)
  - [InitCompressorLZ4](../I/InitCompressorLZ4.md)
  - [InitCompressorZstd](../I/InitCompressorZstd.md)
  - [CompressorState](../C/CompressorState.md)
  - [pg_compress_specification](../p/pg_compress_specification.md)
  - PG_COMPRESSION_NONE
  - PG_COMPRESSION_GZIP
  - PG_COMPRESSION_LZ4
  - PG_COMPRESSION_ZSTD
- Called from (representative examples):
  - [_StartData](../S/_StartData.md) (src/bin/pg_dump/pg_backup_custom.c:297)
  - [_StartLO](../S/_StartLO.md) (src/bin/pg_dump/pg_backup_custom.c:380)
  - [_PrintData](../P/_PrintData.md) (src/bin/pg_dump/pg_backup_custom.c:573)

## Notes and Other Information
- The function allocates memory using pg_malloc0, which zeroes the allocated memory
- The returned CompressorState pointer must be freed by the caller using appropriate cleanup functions
- The specific initialization is delegated to algorithm-specific functions (InitCompressor*)
- The read and write function pointers are stored in the CompressorState for later use during compression operations
- Located in src/bin/pg_dump/compress_io.c at lines 124-148