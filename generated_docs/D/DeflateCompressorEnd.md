# DeflateCompressorEnd

## Location
src/bin/pg_dump/compress_gzip.c: 80 - 101

## Overview
Finalizes and cleans up the deflate compression state, flushing any remaining compressed data and freeing allocated resources.

## Definition
```c
static void DeflateCompressorEnd(ArchiveHandle *AH, CompressorState *cs)
```

## Detailed Description
This function performs the cleanup and finalization of the deflate compression process. It flushes any remaining data in the zlib compression buffer by calling DeflateCompressorCommon with the flush flag set to true, then properly terminates the zlib deflate stream using deflateEnd. After successful cleanup of the compression stream, it deallocates all memory resources including the output buffer, z_stream structure, and the GzipCompressorState itself.

The function ensures proper resource cleanup and prevents memory leaks by freeing all dynamically allocated memory and setting the private_data pointer to NULL.

## Parameters / Member Variables
- `AH`: ArchiveHandle pointer for the pg_dump archive being processed
- `cs`: CompressorState pointer containing the compression state to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - [DeflateCompressorCommon](DeflateCompressorCommon.md) (to flush remaining data)
  - deflateEnd (zlib function to finalize deflate stream)
  - [pg_free](../p/pg_free.md) (PostgreSQL memory deallocation function)
  - [pg_fatal](../p/pg_fatal.md) (for fatal error reporting)
- Types referenced:
  - [ArchiveHandle](../A/ArchiveHandle.md)
  - [CompressorState](../C/CompressorState.md)  
  - [GzipCompressorState](../G/GzipCompressorState.md)
  - z_streamp
- Called from (representative examples):
  - [EndCompressorGzip](../E/EndCompressorGzip.md) (at src/bin/pg_dump/compress_gzip.c:148)

## Notes and Other Information
- Sets zlib stream input pointers to NULL/0 before final flush to ensure no additional input is processed
- Uses DeflateCompressorCommon with flush=true to ensure all buffered data is written out
- Performs comprehensive cleanup of all allocated resources to prevent memory leaks
- Uses pg_fatal for error handling if deflateEnd fails
- Resets cs->private_data to NULL after cleanup to prevent dangling pointer access
- The function is static and located in src/bin/pg_dump/compress_gzip.c:80-101