# bbsink_zstd_begin_backup

## Location
[src/backend/backup/basebackup_zstd.c:88-157](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_zstd.c#L88-L157)

## Overview
Initializes the zstd compression context and configures compression parameters for a basebackup operation, setting up the compression pipeline before data starts flowing.

## Definition


## Detailed Description
This function initializes the zstd compression context for a basebackup sink. It creates a zstd compression context, configures compression parameters based on the compression specification (level, workers, long-distance matching), allocates necessary buffers, and calculates the required output buffer size to accommodate compressed data. The function ensures the next sink in the chain has sufficient buffer space by calculating the compression bound and rounding up to BLCKSZ alignment.

## Parameters / Member Variables
- : Pointer to the bbsink structure (cast to bbsink_zstd internally) that will perform zstd compression

## Dependencies
- Functions called/Symbols referenced:
  - ZSTD_createCCtx (creates zstd compression context)
  - ZSTD_CCtx_setParameter (sets compression parameters)
  - ZSTD_isError (checks for zstd errors)
  - ZSTD_getErrorName (gets error description)
  - ZSTD_compressBound (calculates maximum compressed size)
  - [palloc](../p/palloc.md) (allocates memory for buffer)
  - bbsink_begin_backup (initializes next sink in chain)
  - elog/ereport (error reporting)
  - PG_COMPRESSION_OPTION_WORKERS (worker count option flag)
  - PG_COMPRESSION_OPTION_LONG_DISTANCE (long-distance matching flag)
- Called from (representative examples):
  - Through bbsink_zstd_ops function pointer table

## Notes and Other Information
- Creates and configures zstd compression context with user-specified parameters
- Handles optional worker count setting with graceful fallback for unsupported libzstd versions
- Supports long-distance matching mode when specified in compression options
- Allocates separate buffer for the sink since compressed data differs from input data
- Calculates output buffer bound using ZSTD_compressBound and rounds up to BLCKSZ alignment
- Error handling includes specific error codes for parameter validation failures
- Function is static and called through the bbsink operations table