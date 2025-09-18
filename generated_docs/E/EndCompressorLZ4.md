# EndCompressorLZ4

## Location
src/bin/pg_dump/compress_lz4.c: 238 - 279

## Overview
Finalizes LZ4 compression operations by flushing any remaining compressed data, writing the compression footer, and cleaning up all associated resources.

## Definition
```c
static void EndCompressorLZ4(ArchiveHandle *AH, CompressorState *cs)
```

## Detailed Description
This function performs the cleanup and finalization tasks for LZ4 compression in pg_dump. It handles several critical operations: first, it checks if a header flush is still needed and writes the header if necessary (this can happen when no data was written to the archive). Then it calls LZ4F_compressEnd() to finalize the compression stream and flush any remaining compressed data. After writing the final compressed data, it frees the LZ4 compression context and deallocates all buffers and state structures. The function is designed to be safe to call even if the state is NULL, making it robust for cleanup scenarios.

## Parameters / Member Variables
- `AH`: Pointer to the ArchiveHandle structure representing the archive being finalized
- `cs`: Pointer to the CompressorState structure containing compression state and function pointers

## Dependencies
- Functions called/Symbols referenced:
  - LZ4F_compressEnd
  - LZ4F_freeCompressionContext
  - LZ4F_isError
  - LZ4F_getErrorName
  - pg_fatal
  - pg_free
- Types used:
  - LZ4State
  - CompressorState
- Called from (representative examples):
  - No direct references found (likely used via function pointer)

## Notes and Other Information
- This is a static function internal to the compress_lz4.c module
- Handles the case where no data was written by checking needs_header_flush
- Provides comprehensive error handling for both compression finalization and context cleanup
- Frees all allocated memory including buffers and the LZ4State structure
- Sets cs->private_data to NULL after cleanup to prevent double-free issues
- Part of PostgreSQL's pg_dump LZ4 compression implementation
- Safe to call with NULL state (early return)
- Ensures proper LZ4 frame termination by calling LZ4F_compressEnd()
- Critical for preventing memory leaks and ensuring proper file format compliance