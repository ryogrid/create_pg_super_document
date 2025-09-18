# InitDiscoverCompressFileHandle

## Location
src/bin/pg_dump/compress_io.c: 241 - 289

## Overview
Opens a file for reading while automatically detecting and handling compression format based on file extension or by testing multiple compression formats when the extension is ambiguous.

## Definition
CompressFileHandle *InitDiscoverCompressFileHandle(const char *path, const char *mode)

## Detailed Description
This function provides intelligent file opening capabilities with automatic compression detection for PostgreSQL dump utilities. It first attempts to determine the compression format from the file extension (.gz, .lz4, .zst). If no recognized extension is found, it tries to open the file in multiple ways: first as an uncompressed file, then by appending known compression extensions and testing for file existence. Once the appropriate format is determined, it creates a compression file handle using InitCompressFileHandle and attempts to open the file. The function is specifically designed for reading operations and expects binary read mode.

## Parameters / Member Variables
- path: The base file path to open, which may or may not include a compression extension
- mode: The file opening mode, must be PG_BINARY_R (binary read mode)

## Dependencies
- Functions called/Symbols referenced:
  - pg_strdup
  - hasSuffix
  - stat
  - check_compressed_file
  - InitCompressFileHandle
  - free_keep_errno
  - PG_COMPRESSION_NONE
  - PG_COMPRESSION_GZIP
  - PG_COMPRESSION_LZ4
  - PG_COMPRESSION_ZSTD
  - PG_BINARY_R
- Called from (representative examples):
  - InitArchiveFmt_Directory
  - _PrintFileData
  - _LoadLOs

## Notes and Other Information
The function implements a fallback strategy for compression detection: explicit extension recognition first, then probing for compressed variants if the base file doesn't exist. It only supports reading mode and includes an assertion to enforce this. The function properly manages memory allocation and cleanup, using free_keep_errno to preserve error codes. If opening fails, the function returns NULL and sets errno appropriately.