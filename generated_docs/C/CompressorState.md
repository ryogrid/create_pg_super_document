# CompressorState

## Location
src/bin/pg_dump/compress_io.h: 49 - 50

## Overview
CompressorState is a structure that provides an abstraction layer for compression operations in pg_dump, encapsulating function pointers and data needed for reading, writing, and managing compressed data streams.

## Definition
```c
typedef struct CompressorState CompressorState;
struct CompressorState
{
    void (*readData)(ArchiveHandle *AH, CompressorState *cs);
    void (*writeData)(ArchiveHandle *AH, CompressorState *cs, const void *data, size_t dLen);
    void (*end)(ArchiveHandle *AH, CompressorState *cs);
    ReadFunc readF;
    WriteFunc writeF;
    pg_compress_specification compression_spec;
    void *private_data;
};
```

## Detailed Description
CompressorState serves as a unified interface for different compression algorithms used in pg_dump. It implements a strategy pattern where specific compression implementations (gzip, LZ4, zstd, none) provide their own implementations of the function pointers. The structure manages the lifecycle of compressed data streams, from initialization through data processing to cleanup.

## Parameters / Member Variables
- `readData`: Function pointer to read all compressed data from input stream and output with ahwrite()
- `writeData`: Function pointer to compress and write data to output stream via writeF
- `end`: Function pointer to end compression and flush any internal buffers
- `readF`: Callback function to read from an already processed input stream
- `writeF`: Callback function to write an already processed chunk of data
- `compression_spec`: Compression specification containing algorithm and parameters
- `private_data`: Private data pointer for compressor-specific state information

## Dependencies
- Functions called/Symbols referenced:
  - pg_compress_specification
  - ArchiveHandle
  - ReadFunc
  - WriteFunc
- Called from (representative examples):
  - AllocateCompressor (src/bin/pg_dump/compress_io.c:127)
  - EndCompressor (src/bin/pg_dump/compress_io.c:149)
  - InitCompressorGzip (src/bin/pg_dump/compress_gzip.c:425)
  - InitCompressorLZ4 (src/bin/pg_dump/compress_lz4.c:797)
  - InitCompressorZstd (src/bin/pg_dump/compress_zstd.c:212)
  - InitCompressorNone (src/bin/pg_dump/compress_none.c:66)

## Notes and Other Information
- Defined in src/bin/pg_dump/compress_io.h:49-88
- Used extensively across multiple compression implementations (gzip, LZ4, zstd, none)
- The structure enables pluggable compression algorithms without changing client code
- Private data field allows each compression implementation to maintain its own state
- Part of the pg_dump compression infrastructure for backup/restore operations