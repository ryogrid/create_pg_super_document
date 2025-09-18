# bbsink_lz4

## Location
src/backend/backup/basebackup_lz4.c: 23 - 36

## Overview
A specialized basebackup sink structure that implements LZ4 compression for PostgreSQL base backup archives and manifest files.

## Definition


## Detailed Description
The  structure extends the base  functionality to provide LZ4 compression capabilities for PostgreSQL base backups. This structure is part of the basebackup sink chain architecture, where each sink performs a specific task such as compression, progress reporting, or client communication.

This sink compresses backup archives and manifest data using the LZ4 compression algorithm, which provides fast compression/decompression speeds with reasonable compression ratios. The structure maintains LZ4-specific compression context and preferences, along with tracking the number of bytes written to the output buffer.

The implementation is conditionally compiled and only available when PostgreSQL is built with LZ4 support (USE_LZ4 preprocessor macro).

## Parameters / Member Variables
- : Base  structure containing common sink functionality including operation callbacks, buffer management, and state tracking
- : Integer specifying the LZ4 compression level to use for compressing the backup data
- : LZ4 compression context handle used by the LZ4 frame API for maintaining compression state across multiple data chunks
- : LZ4 compression preferences structure that configures compression parameters such as block size, checksum options, and compression level
- : Size counter tracking the number of bytes that have been staged in the output buffer for the current operation

## Dependencies
- Functions called/Symbols referenced:
  - bbsink (base structure)
  - LZ4F_compressionContext_t (LZ4 library type)
  - LZ4F_preferences_t (LZ4 library type)

- Called from (representative examples):
  - bbsink_lz4_new (constructor function)
  - bbsink_lz4_begin_backup (backup initialization)
  - bbsink_lz4_begin_archive (archive initialization)
  - bbsink_lz4_archive_contents (content compression)
  - bbsink_lz4_end_archive (archive finalization)
  - bbsink_lz4_cleanup (cleanup operations)

## Notes and Other Information
- This structure is only available when PostgreSQL is compiled with LZ4 support ()
- Part of the basebackup sink chain architecture that allows composable processing of backup data
- Uses the LZ4 Frame API for streaming compression, which provides better integration with the sink pipeline
- The compression is applied to both tablespace archives and the backup manifest
- Located in 
- Implements the standard bbsink operation callbacks for LZ4-specific processing