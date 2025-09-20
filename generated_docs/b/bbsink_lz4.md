# bbsink_lz4

## Location
[src/backend/backup/basebackup_lz4.c:23-36](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_lz4.c#L23-L36)

## Overview
A specialized basebackup sink structure that implements LZ4 compression for PostgreSQL base backup archives and manifest files.

## Definition

```c
typedef struct bbsink_lz4
{
	/* Common information for all types of sink. */
	bbsink		base;

	/* Compression level. */
	int			compresslevel;

	LZ4F_compressionContext_t ctx;
	LZ4F_preferences_t prefs;

	/* Number of bytes staged in output buffer. */
	size_t		bytes_written;
} bbsink_lz4;
```
## Detailed Description
The  structure extends the base  functionality to provide LZ4 compression capabilities for PostgreSQL base backups. This structure is part of the basebackup sink chain architecture, where each sink performs a specific task such as compression, progress reporting, or client communication.

This sink compresses backup archives and manifest data using the LZ4 compression algorithm, which provides fast compression/decompression speeds with reasonable compression ratios. The structure maintains LZ4-specific compression context and preferences, along with tracking the number of bytes written to the output buffer.

The implementation is conditionally compiled and only available when PostgreSQL is built with LZ4 support (USE_LZ4 preprocessor macro).

## Parameters / Member Variables
- `base`: Base  structure containing common sink functionality including operation callbacks, buffer management, and state tracking
- `compresslevel`: Integer specifying the LZ4 compression level to use for compressing the backup data
- `ctx`: LZ4 compression context handle used by the LZ4 frame API for maintaining compression state across multiple data chunks
- `prefs`: LZ4 compression preferences structure that configures compression parameters such as block size, checksum options, and compression level
- `bytes_written`: Size counter tracking the number of bytes that have been staged in the output buffer for the current operation
## Dependencies
- Functions called/Symbols referenced:
  - bbsink (base structure)
  - LZ4F_compressionContext_t (LZ4 library type)
  - LZ4F_preferences_t (LZ4 library type)

- Called from (representative examples):
  - [bbsink_lz4_new](bbsink_lz4_new.md) (constructor function)
  - [bbsink_lz4_begin_backup](bbsink_lz4_begin_backup.md) (backup initialization)
  - [bbsink_lz4_begin_archive](bbsink_lz4_begin_archive.md) (archive initialization)
  - [bbsink_lz4_archive_contents](bbsink_lz4_archive_contents.md) (content compression)
  - [bbsink_lz4_end_archive](bbsink_lz4_end_archive.md) (archive finalization)
  - [bbsink_lz4_cleanup](bbsink_lz4_cleanup.md) (cleanup operations)

## Notes and Other Information
- This structure is only available when PostgreSQL is compiled with LZ4 support ()
- Part of the basebackup sink chain architecture that allows composable processing of backup data
- Uses the LZ4 Frame API for streaming compression, which provides better integration with the sink pipeline
- The compression is applied to both tablespace archives and the backup manifest
- Located in 
- Implements the standard bbsink operation callbacks for LZ4-specific processing