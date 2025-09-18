# Zstd_get_error

## Location
src/bin/pg_dump/compress_zstd.c: 551 - 558

## Overview
Retrieves the last error message from a zstd compression file handle by accessing the error string stored in the compression state.

## Definition


## Detailed Description
This function provides access to error information for zstd compression operations within PostgreSQL's pg_dump utility. It extracts the ZstdCompressorState from the CompressFileHandle's private data and returns the stored error message string. The function serves as the error reporting mechanism for the zstd compression backend, allowing calling code to retrieve descriptive error messages when compression operations fail.

## Parameters / Member Variables
- `CFH`: Pointer to the CompressFileHandle structure containing the zstd compression state in its private_data field

## Dependencies
- Functions called/Symbols referenced:
  - [CompressFileHandle](../C/CompressFileHandle.md) (structure)
  - [ZstdCompressorState](ZstdCompressorState.md) (structure)
- Called from (representative examples):
  - [InitCompressFileHandleZstd](../I/InitCompressFileHandleZstd.md) (assigned as get_error_func)

## Notes and Other Information
- This is a static function local to compress_zstd.c
- Returns a pointer to the zstderror field in ZstdCompressorState
- The returned string is typically a static string from error reporting functions
- Part of the unified error handling interface for compression backends in pg_dump
- The error string persists until the next operation that might update it