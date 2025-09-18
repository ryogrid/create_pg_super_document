# DeflateCompressorCommon

## Location
src/bin/pg_dump/compress_gzip.c: 102 - 143

## Overview
Core compression worker function that handles the actual deflate compression process and manages output buffer operations.

## Definition
```c
static void DeflateCompressorCommon(ArchiveHandle *AH, CompressorState *cs, bool flush)
```

## Detailed Description
This function performs the actual zlib deflate compression work, processing input data through the deflate algorithm and managing the output buffer. It operates in a loop, continuously compressing data while there is input available or when a flush is requested. The function handles buffer management by writing compressed data to the archive when the output buffer fills up or when specific conditions are met.

The function includes paranoid checks to avoid zero-length chunks, which would be interpreted as EOF markers in the custom format. It uses different deflate modes (Z_FINISH for flushing vs Z_NO_FLUSH for normal operation) and properly handles the compressed data output through the compressor's writeF callback.

## Parameters / Member Variables
- `AH`: ArchiveHandle pointer for the pg_dump archive being processed
- `cs`: CompressorState pointer containing compression configuration and callback functions
- `flush`: Boolean flag indicating whether to flush remaining data (uses Z_FINISH mode)

## Dependencies
- Functions called/Symbols referenced:
  - deflate (zlib compression function)
  - [pg_fatal](../p/pg_fatal.md) (for fatal error reporting)
  - cs->writeF (callback function for writing compressed data)
- Types referenced:
  - [ArchiveHandle](../A/ArchiveHandle.md)
  - [CompressorState](../C/CompressorState.md)
  - [GzipCompressorState](../G/GzipCompressorState.md)
  - z_streamp
- Zlib constants used:
  - Z_OK, Z_STREAM_ERROR, Z_STREAM_END
  - Z_FINISH, Z_NO_FLUSH
- Called from (representative examples):
  - [DeflateCompressorEnd](DeflateCompressorEnd.md) (at src/bin/pg_dump/compress_gzip.c:90)
  - [WriteDataToArchiveGzip](../W/WriteDataToArchiveGzip.md) (at src/bin/pg_dump/compress_gzip.c:159)

## Notes and Other Information
- Implements a loop that continues until all input is processed or stream ends
- Uses Z_FINISH mode when flush=true for final data output, Z_NO_FLUSH for normal compression
- Includes paranoid checks to prevent zero-length chunks that could be mistaken for EOF markers
- Manages output buffer by resetting pointers and available space after writing data
- Performs error checking on deflate operations and uses pg_fatal for stream errors
- Writes compressed data through the CompressorState's writeF callback function
- The function is static and located in src/bin/pg_dump/compress_gzip.c:102-143