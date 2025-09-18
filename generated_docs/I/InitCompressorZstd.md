# InitCompressorZstd

## Location
src/bin/pg_dump/compress_zstd.c: 212 - 261

## Overview
InitCompressorZstd is the public interface function that initializes Zstd compression/decompression support for pg_dump archives, setting up the appropriate data structures and function pointers based on whether reading or writing is required.

## Definition
void InitCompressorZstd(CompressorState *cs, const pg_compress_specification compression_spec)

## Detailed Description
This function serves as the entry point for initializing Zstd compression support in pg_dump. It configures the CompressorState with appropriate function pointers for Zstd operations (readData, writeData, end) and allocates the necessary private data structures. The function determines whether to initialize for compression (writing) or decompression (reading) based on which function pointer (writeF or readF) is provided in the CompressorState. For reading operations, it creates a decompression stream and allocates input/output buffers sized according to Zstd recommendations. For writing operations, it initializes a compression stream with the specified compression parameters.

## Parameters / Member Variables
- : CompressorState structure to be initialized with Zstd-specific operations and data
- : Compression specification containing parameters like compression level and other options

## Dependencies
- Functions called/Symbols referenced:
  - [CompressorState](../C/CompressorState.md) (struct type)
  - [pg_compress_specification](../p/pg_compress_specification.md) (struct type)
  - [ZstdCompressorState](../Z/ZstdCompressorState.md) (struct type)
  - [ReadDataFromArchiveZstd](../R/ReadDataFromArchiveZstd.md) (function pointer assignment)
  - [WriteDataToArchiveZstd](../W/WriteDataToArchiveZstd.md) (function pointer assignment)
  - [EndCompressorZstd](../E/EndCompressorZstd.md) (function pointer assignment)
  - pg_malloc0 (memory allocation)
  - pg_malloc (memory allocation)
  - [_ZstdCStreamParams](../Z/_ZstdCStreamParams.md) (Zstd stream parameter setup)
  - ZSTD_createDStream() (Zstd library function)
  - ZSTD_DStreamInSize() (Zstd library function)
  - ZSTD_DStreamOutSize() (Zstd library function)
  - ZSTD_CStreamOutSize() (Zstd library function)
- Called from (representative examples):
  - [AllocateCompressor](../A/AllocateCompressor.md) (from compress_io.c)

## Notes and Other Information
- Uses assertions to ensure exactly one of readF or writeF is specified, preventing ambiguous initialization
- Allocates an extra byte for the output buffer in read mode to support null-terminated strings for ExecuteSqlCommandBuf() optimization
- The private_data field is used to store the ZstdCompressorState containing Zstd-specific context and buffers
- Error handling uses pg_fatal() for critical initialization failures
- The function is designed to work with both compression and decompression workflows in pg_dump