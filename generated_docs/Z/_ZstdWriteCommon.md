# _ZstdWriteCommon

## Location
[src/bin/pg_dump/compress_zstd.c:94-125](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_zstd.c#L94-L125)

## Overview
A static helper function that handles the common compression and output logic for both data writing and compression finalization in the ZSTD compression implementation.

## Definition
```c
static void _ZstdWriteCommon(ArchiveHandle *AH, CompressorState *cs, bool flush)
```

## Detailed Description
This function encapsulates the core ZSTD compression loop used by both `WriteDataToArchiveZstd` and `EndCompressorZstd`. It processes input data through the ZSTD compression stream and writes the compressed output to the archive. The function continues processing until all input is consumed or, when flushing, until the compression stream is properly finalized. It includes safety checks to avoid writing zero-length chunks, which could be misinterpreted as EOF markers in the custom archive format.

## Parameters / Member Variables
- `AH`: Pointer to the ArchiveHandle containing archive state and output functions
- `cs`: Pointer to the CompressorState containing compression context and buffers
- `flush`: Boolean flag indicating whether to finalize the compression stream (ZSTD_e_end) or continue processing (ZSTD_e_continue)

## Dependencies
- Functions called/Symbols referenced:
  - [ZstdCompressorState](ZstdCompressorState.md) (cast target for private_data)
  - ZSTD_compressStream2 (from ZSTD library)
  - ZSTD_isError (from ZSTD library)
  - [pg_fatal](../p/pg_fatal.md) (PostgreSQL error handling)
  - ZSTD_getErrorName (from ZSTD library)
  - [CompressorState](../C/CompressorState.md) (compression state structure)
- Called from (representative examples):
  - [EndCompressorZstd](../E/EndCompressorZstd.md)
  - [WriteDataToArchiveZstd](../W/WriteDataToArchiveZstd.md)

## Notes and Other Information
- This is a static function internal to the compress_zstd.c module
- Implements paranoid safety check to prevent zero-length output chunks
- Uses ZSTD_e_end mode when flushing to properly finalize compression
- Uses ZSTD_e_continue mode for normal data processing
- The loop continues until either all input is processed or compression is complete (res == 0)
- Critical for proper ZSTD compression flow in PostgreSQL's pg_dump utility
- Handles both incremental compression and final compression phases