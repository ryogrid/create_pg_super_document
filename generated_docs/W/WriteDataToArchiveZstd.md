# WriteDataToArchiveZstd

## Location
src/bin/pg_dump/compress_zstd.c: 149 - 161

## Overview
A function that compresses input data using ZSTD compression and writes it to the archive as part of PostgreSQL's pg_dump compression pipeline.

## Definition
```c
static void WriteDataToArchiveZstd(ArchiveHandle *AH, CompressorState *cs, const void *data, size_t dLen)
```

## Detailed Description
This function serves as the main entry point for compressing data using ZSTD in PostgreSQL's pg_dump utility. It takes raw input data and prepares it for compression by setting up the input buffer in the ZstdCompressorState structure, then delegates the actual compression work to the `_ZstdWriteCommon` helper function. The function is designed to handle incremental data compression as part of a streaming compression process.

## Parameters / Member Variables
- `AH`: Pointer to the ArchiveHandle containing archive context and output functions
- `cs`: Pointer to the CompressorState containing compression state and configuration
- `data`: Pointer to the raw data to be compressed
- `dLen`: Size of the input data in bytes

## Dependencies
- Functions called/Symbols referenced:
  - ZstdCompressorState (cast target for private_data)
  - _ZstdWriteCommon (internal helper for compression processing)
  - CompressorState (compression state structure)
- Called from (representative examples):
  - InitCompressorZstd (registered as data write callback)

## Notes and Other Information
- This is a static function internal to the compress_zstd.c module
- Sets up input buffer parameters (src, size, pos) for ZSTD compression
- Always calls `_ZstdWriteCommon` with flush=false for incremental compression
- Part of the callback-based compression architecture in PostgreSQL's pg_dump
- The actual compression and output writing is handled by the shared `_ZstdWriteCommon` function
- Registered as a callback function during compressor initialization for handling data writes
- Works in coordination with `EndCompressorZstd` which handles final compression flushing