# InitCompressFileHandle

## Location
src/bin/pg_dump/compress_io.c: 195 - 219

## Overview
Initializes a compress file handle for the specified compression algorithm, serving as a factory function that creates and configures compression handlers based on the requested algorithm type.

## Definition
CompressFileHandle *InitCompressFileHandle(const pg_compress_specification compression_spec)

## Detailed Description
This function acts as a factory method for creating compression file handles in PostgreSQL's dump utilities. It allocates memory for a CompressFileHandle structure and initializes it according to the specified compression algorithm. The function supports multiple compression algorithms including no compression, gzip, LZ4, and Zstd. Each algorithm has its own specialized initialization function that configures the handle with algorithm-specific settings and function pointers.

## Parameters / Member Variables
- compression_spec: A pg_compress_specification structure containing the compression algorithm type and related configuration parameters

## Dependencies
- Functions called/Symbols referenced:
  - pg_malloc0
  - InitCompressFileHandleNone
  - InitCompressFileHandleGzip  
  - InitCompressFileHandleLZ4
  - InitCompressFileHandleZstd
  - PG_COMPRESSION_NONE
  - PG_COMPRESSION_GZIP
  - PG_COMPRESSION_LZ4
  - PG_COMPRESSION_ZSTD
- Called from (representative examples):
  - InitDiscoverCompressFileHandle
  - SetOutput
  - _allocAH
  - _StartData
  - _CloseArchive
  - _StartLOs
  - _StartLO

## Notes and Other Information
The function uses a switch-like pattern based on the compression algorithm to delegate initialization to algorithm-specific functions. The returned CompressFileHandle contains function pointers and state specific to the chosen compression method, enabling polymorphic behavior for compression operations throughout the pg_dump utilities.