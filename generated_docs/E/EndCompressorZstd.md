# EndCompressorZstd

## Location
[src/bin/pg_dump/compress_zstd.c:126-148](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_zstd.c#L126-L148)

## Overview
A cleanup function that finalizes ZSTD compression operations and releases all associated resources for both compression and decompression modes.

## Definition
```c
static void EndCompressorZstd(ArchiveHandle *AH, CompressorState *cs)
```

## Detailed Description
This function serves as the cleanup and finalization routine for ZSTD compression operations in PostgreSQL's pg_dump utility. It handles both compression and decompression modes by checking the presence of read/write function pointers in the CompressorState. For compression mode (writeF present), it calls `_ZstdWriteCommon` with flush=true to finalize the compression stream before freeing the compression context. For decompression mode (readF present), it directly frees the decompression context and input buffer. In both cases, it ensures proper cleanup of output buffers and the ZstdCompressorState structure.

## Parameters / Member Variables
- `AH`: Pointer to the ArchiveHandle containing archive context
- `cs`: Pointer to the CompressorState containing compression/decompression state and configuration

## Dependencies
- Functions called/Symbols referenced:
  - [ZstdCompressorState](../Z/ZstdCompressorState.md) (cast target for private_data)
  - Assert (debugging assertion)
  - ZSTD_freeDStream (from ZSTD library for decompression cleanup)
  - [pg_free](../p/pg_free.md) (PostgreSQL memory management)
  - unconstify (PostgreSQL utility for const casting)
  - [_ZstdWriteCommon](../Z/_ZstdWriteCommon.md) (internal helper for compression finalization)
  - ZSTD_freeCStream (from ZSTD library for compression cleanup)
  - [CompressorState](../C/CompressorState.md) (compression state structure)
- Called from (representative examples):
  - [InitCompressorZstd](../I/InitCompressorZstd.md) (registered as cleanup callback)

## Notes and Other Information
- This is a static function internal to the compress_zstd.c module
- Handles dual-mode operation: both compression (writeF) and decompression (readF)
- Uses assertions to ensure stream contexts are properly isolated (cstream vs dstream)
- Flushes remaining compressed data before cleanup in compression mode
- Always frees output buffer regardless of mode since it may be allocated in either case
- Critical for preventing memory leaks in PostgreSQL's compression subsystem
- Registered as a callback function during compressor initialization