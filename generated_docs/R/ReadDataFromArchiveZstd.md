# ReadDataFromArchiveZstd

## Location
[src/bin/pg_dump/compress_zstd.c:162-211](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_zstd.c#L162-L211)

## Overview
ReadDataFromArchiveZstd is a static function that reads and decompresses Zstd-compressed data from an archive, serving as the core decompression routine for pg_dump's Zstd compression support.

## Definition
static void ReadDataFromArchiveZstd(ArchiveHandle *AH, CompressorState *cs)

## Detailed Description
This function implements the data reading and decompression logic for Zstd-compressed archives in pg_dump. It operates in a continuous loop to read compressed data chunks, decompress them using the Zstd library, and write the decompressed output to the archive handle. The function manages input/output buffers efficiently, handling the streaming decompression process by reading data in appropriately sized chunks determined by ZSTD_DStreamInSize() and processing them through ZSTD_decompressStream(). The decompressed data is null-terminated before being written to optimize ExecuteSqlCommandBuf() performance.

## Parameters / Member Variables
- : Archive handle for reading compressed data and writing decompressed output
- : Compressor state containing the private Zstd decompression context and buffers

## Dependencies
- Functions called/Symbols referenced:
  - [CompressorState](../C/CompressorState.md) (struct type)
  - [ZstdCompressorState](../Z/ZstdCompressorState.md) (struct type)
  - unconstify (utility function)
  - [ahwrite](../a/ahwrite.md) (archive write function)
  - ZSTD_DStreamInSize() (Zstd library function)
  - ZSTD_decompressStream() (Zstd library function)
  - ZSTD_isError() (Zstd library function)
  - ZSTD_getErrorName() (Zstd library function)
- Called from (representative examples):
  - [InitCompressorZstd](../I/InitCompressorZstd.md) (assigned as cs->readData function pointer)

## Notes and Other Information
- The function is designed to handle variable-sized input buffers, with readF potentially resizing the buffer during operation
- Includes assertions to ensure the input buffer is never shrunk by readF
- Adds null termination to decompressed output for optimization in SQL command execution
- Uses pg_fatal() for error reporting when decompression fails
- Handles end-of-frame detection (res == 0) to properly terminate decompression loops