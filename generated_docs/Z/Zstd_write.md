# Zstd_write

## Location
src/bin/pg_dump/compress_zstd.c: 354 - 393

## Overview
Zstd_write is a static function that handles compression and writing of data to Zstd-compressed files, implementing the core compression logic for pg_dump's compressed stream API.

## Definition
static void Zstd_write(const void *ptr, size_t size, CompressFileHandle *CFH)

## Detailed Description
This function implements the writing mechanism for Zstd-compressed files by compressing input data and writing it to the underlying file. It manages the compression process using Zstd's streaming API, handling lazy initialization of the compression stream on first write, and processing data through input/output buffers. The function ensures all input data is consumed by operating in a loop that compresses data incrementally and writes the compressed output to the file. It uses ZSTD_e_continue mode to indicate that more data may follow, allowing for efficient streaming compression without finalizing the stream.

## Parameters / Member Variables
- : Pointer to the input data buffer to be compressed and written
- : Number of bytes to compress and write from the input buffer
- : Compressed file handle containing Zstd private data, compression parameters, and file pointer

## Dependencies
- Functions called/Symbols referenced:
  - CompressFileHandle (struct type)
  - ZstdCompressorState (struct type)
  - pg_malloc0 (memory allocation)
  - _ZstdCStreamParams (Zstd stream parameter setup)
  - ZSTD_compressStream2() (Zstd library function)
  - ZSTD_isError() (Zstd library function)
  - ZSTD_getErrorName() (Zstd library function)
  - ZSTD_CStreamOutSize() (Zstd library function)
  - fwrite() (standard library function)
- Called from (representative examples):
  - InitCompressFileHandleZstd (assigned as write function pointer)

## Notes and Other Information
- Implements lazy initialization of the compression stream, creating it only when first write occurs
- Uses ZSTD_e_continue mode for streaming compression, indicating that more data may follow
- Provides comprehensive error handling for both compression failures and file write errors
- Sets errno appropriately for write failures, defaulting to ENOSPC if errno is not set
- Ensures all input data is consumed before returning, maintaining data integrity
- The function integrates with PostgreSQL's error reporting system using pg_fatal()
- Output buffer is allocated based on Zstd's recommended output size for optimal performance