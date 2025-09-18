# InitCompressorGzip

## Location
[src/bin/pg_dump/compress_gzip.c:425-431](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_gzip.c#L425-L431)

## Overview
Initializes a CompressorState structure for gzip compression, setting up function pointers and internal state for compressing data streams using the zlib library.

## Definition


## Detailed Description
InitCompressorGzip is a public API function that configures a CompressorState structure to use gzip compression. It serves as the entry point for setting up gzip-based compression in PostgreSQL's pg_dump utility. The function has two implementations depending on build configuration:

1. **When HAVE_LIBZ is defined**: Sets up function pointers for gzip compression operations and initializes the compression state if a write function is provided.
2. **When HAVE_LIBZ is not defined**: Terminates with a fatal error indicating that gzip support was not compiled in.

The function assigns specific gzip-related handlers for reading, writing, and cleanup operations, copies the compression specification, and conditionally initializes the deflate compressor for write operations.

## Parameters / Member Variables
- : Pointer to a CompressorState structure to be initialized with gzip compression capabilities
- : Structure containing compression parameters including compression level and other options

## Dependencies
- Functions called/Symbols referenced:
  - [ReadDataFromArchiveGzip](../R/ReadDataFromArchiveGzip.md)
  - [WriteDataToArchiveGzip](../W/WriteDataToArchiveGzip.md)  
  - [EndCompressorGzip](../E/EndCompressorGzip.md)
  - [DeflateCompressorInit](../D/DeflateCompressorInit.md)
  - [pg_fatal](../p/pg_fatal.md)
- Called from (representative examples):
  - [AllocateCompressor](../A/AllocateCompressor.md) (src/bin/pg_dump/compress_io.c:136)

## Notes and Other Information
- The function is conditionally compiled based on HAVE_LIBZ preprocessor definition
- When zlib is not available, calling this function results in program termination with an error message
- If the CompressorState has a writeF function defined, deflate compression is immediately initialized
- The function supports both compression and decompression operations through the function pointers it sets
- Part of PostgreSQL's modular compression system in pg_dump, allowing different compression algorithms to be plugged in
- Located in src/bin/pg_dump/compress_gzip.c:230-246 (HAVE_LIBZ version) and lines 425-429 (no-libz version)