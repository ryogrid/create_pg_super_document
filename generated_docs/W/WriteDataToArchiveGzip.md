# WriteDataToArchiveGzip

## Location
[src/bin/pg_dump/compress_gzip.c:152-162](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_gzip.c#L152-L162)

## Overview
Public interface function for compressing and writing data to the archive using gzip compression.

## Definition
```c
static void WriteDataToArchiveGzip(ArchiveHandle *AH, CompressorState *cs, const void *data, size_t dLen)
```

## Detailed Description
This function serves as the primary interface for writing data through the gzip compression system in pg_dump. It takes raw input data and sets up the zlib stream to process it by configuring the input buffer pointers and size, then delegates the actual compression work to DeflateCompressorCommon. The function acts as a simple adapter that prepares the zlib stream with the provided data and initiates the compression process without flushing.

This is typically called repeatedly during the dump process as data needs to be compressed and written to the archive file.

## Parameters / Member Variables
- `AH`: ArchiveHandle pointer for the pg_dump archive being processed
- `cs`: CompressorState pointer containing compression configuration and state
- `data`: Pointer to the raw data buffer to be compressed
- `dLen`: Size of the data buffer in bytes

## Dependencies
- Functions called/Symbols referenced:
  - [DeflateCompressorCommon](../D/DeflateCompressorCommon.md) (for actual compression processing)
- Types referenced:
  - [ArchiveHandle](../A/ArchiveHandle.md)
  - [CompressorState](../C/CompressorState.md)
  - [GzipCompressorState](../G/GzipCompressorState.md)
- Called from (representative examples):
  - No direct references found (likely used via function pointer in compression interface)

## Notes and Other Information
- Sets up zlib stream input pointers (next_in, avail_in) with the provided data
- Calls DeflateCompressorCommon with flush=false for normal compression operation
- Acts as the main entry point for data compression in the gzip compression interface
- Simple adapter function that prepares input and delegates to the compression worker
- Part of the pg_dump compression callback interface system
- The function is static and located in src/bin/pg_dump/compress_gzip.c:152-162

## Simplified Source

```c
static void WriteDataToArchiveGzip(ArchiveHandle *AH, CompressorState *cs,
                                   const void *data, size_t dLen)
{
    GzipCompressorState *gzipcs = (GzipCompressorState *) cs->private_data;

    // Set up input data for compression
    gzipcs->zp->next_in = data;
    gzipcs->zp->avail_in = dLen;

    // Compress the data (no flush)
    DeflateCompressorCommon(AH, cs, false);
}
```