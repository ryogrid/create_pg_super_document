# bbsink_zstd

## Location
[src/backend/backup/basebackup_zstd.c:23-33](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_zstd.c#L23-L33)

## Overview
A PostgreSQL structure that represents a base backup sink implementing zstd compression, extending the base bbsink functionality to compress archive data during backup operations.

## Definition

```c
typedef struct bbsink_zstd
{
	/* Common information for all types of sink. */
	bbsink		base;

	/* Compression options */
	pg_compress_specification *compress;

	ZSTD_CCtx  *cctx;
	ZSTD_outBuffer zstd_outBuf;
} bbsink_zstd;
```
## Detailed Description
The bbsink_zstd structure is a specialized implementation of the base backup sink system in PostgreSQL that provides zstd compression capabilities. It is part of PostgreSQL's modular backup architecture where different sink types can be chained together to process backup data through various stages (compression, throttling, progress reporting, etc.).

This sink operates by:
1. Receiving uncompressed backup archive data
2. Compressing it using the zstd compression library
3. Forwarding the compressed data to the next sink in the chain
4. Adding ".zst" extension to archive names to indicate compression

The structure maintains its own compression context and output buffer to handle the streaming compression of backup data efficiently. It supports various zstd compression options including compression level, worker threads, and long-distance matching.

## Parameters / Member Variables
- `base`: The base bbsink structure containing common sink functionality including operation callbacks, buffer management, and chain linking
- `*compress`: Pointer to compression specification containing compression parameters like level, worker count, and compression options
- `*cctx`: zstd compression context used to maintain compression state across multiple compression operations
- `zstd_outBuf`: zstd output buffer structure that manages the compressed data output, containing destination buffer, size, and current position
## Dependencies
- Functions called/Symbols referenced:
  - bbsink (base structure)
  - [pg_compress_specification](../p/pg_compress_specification.md) (compression options)
  - ZSTD_CCtx (zstd compression context)
  - ZSTD_outBuffer (zstd output buffer)

- Called from (representative examples):
  - [bbsink_zstd_new](bbsink_zstd_new.md) (constructor function)
  - [bbsink_zstd_begin_backup](bbsink_zstd_begin_backup.md) (backup initialization)
  - [bbsink_zstd_begin_archive](bbsink_zstd_begin_archive.md) (archive processing start)
  - [bbsink_zstd_archive_contents](bbsink_zstd_archive_contents.md) (data compression)
  - [bbsink_zstd_end_archive](bbsink_zstd_end_archive.md) (archive finalization)
  - [bbsink_zstd_end_backup](bbsink_zstd_end_backup.md) (backup completion)
  - [bbsink_zstd_cleanup](bbsink_zstd_cleanup.md) (resource cleanup)

## Notes and Other Information
- Only available when PostgreSQL is compiled with zstd support (USE_ZSTD compile flag)
- Implements the bbsink_ops callback interface with zstd-specific implementations
- Supports streaming compression allowing for memory-efficient processing of large backup archives  
- Manifest contents are not compressed but passed through to maintain backup integrity
- Automatically handles buffer management and forwards compressed data to the next sink in the chain
- Compression parameters are configurable through the pg_compress_specification structure
- Provides proper error handling for zstd library operations with PostgreSQL's error reporting system