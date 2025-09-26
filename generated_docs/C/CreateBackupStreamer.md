# CreateBackupStreamer

## Location
[src/bin/pg_basebackup/pg_basebackup.c:1061-1283](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_basebackup.c#L1061-L1283)

## Overview
A comprehensive factory function that constructs a chain of backup streaming components based on user options, handling compression, extraction, manifest injection, and various output formats for PostgreSQL base backups.

## Definition
```c
static bbstreamer *CreateBackupStreamer(char *archive_name, char *spclocation, bbstreamer **manifest_inject_streamer_p, bool is_recovery_guc_supported, bool expect_unterminated_tarfile, pg_compress_specification *compress)
```

## Detailed Description
This complex function serves as the central orchestrator for creating appropriate backup streaming pipelines in pg_basebackup. It analyzes user-specified options and archive characteristics to construct a chain of bbstreamer objects that will process backup data appropriately. The function handles multiple scenarios including:

1. **Format decisions**: Plain directory extraction vs tar archive creation
2. **Compression handling**: Client-side and server-side compression with multiple algorithms (gzip, lz4, zstd)
3. **Archive parsing**: When to parse tar archives for manifest injection or recovery configuration
4. **Output routing**: Writing to files, directories, or standard output
5. **Special processing**: Backup manifest injection and recovery configuration injection

The function builds a processing pipeline by chaining together appropriate bbstreamer components, each responsible for a specific transformation or output operation. The order of chaining is carefully designed to ensure proper data flow and processing.

## Parameters / Member Variables
- `archive_name`: Name of the archive file being processed (used to determine file type and compression)
- `spclocation`: Tablespace location string, NULL for main tablespace
- `manifest_inject_streamer_p`: Output parameter returning the streamer where manifest injection should occur
- `is_recovery_guc_supported`: Boolean indicating if the target server supports recovery GUC parameters
- `expect_unterminated_tarfile`: Boolean indicating if the server sends improperly terminated tar files
- `compress`: Compression specification structure containing algorithm and parameters

## Dependencies
- Functions called/Symbols referenced:
  - [bbstreamer_extractor_new](../b/bbstreamer_extractor_new.md) (creates directory extraction streamer)
  - [bbstreamer_plain_writer_new](../b/bbstreamer_plain_writer_new.md) (creates plain file writer)
  - [bbstreamer_gzip_writer_new](../b/bbstreamer_gzip_writer_new.md) (creates gzip compression writer)
  - [bbstreamer_lz4_compressor_new](../b/bbstreamer_lz4_compressor_new.md) (creates lz4 compressor)
  - [bbstreamer_zstd_compressor_new](../b/bbstreamer_zstd_compressor_new.md) (creates zstd compressor)
  - [bbstreamer_tar_archiver_new](../b/bbstreamer_tar_archiver_new.md) (creates tar archive writer)
  - [bbstreamer_tar_parser_new](../b/bbstreamer_tar_parser_new.md) (creates tar parser)
  - [bbstreamer_tar_terminator_new](../b/bbstreamer_tar_terminator_new.md) (creates tar terminator)
  - [bbstreamer_gzip_decompressor_new](../b/bbstreamer_gzip_decompressor_new.md) (creates gzip decompressor)
  - [bbstreamer_lz4_decompressor_new](../b/bbstreamer_lz4_decompressor_new.md) (creates lz4 decompressor)
  - [bbstreamer_zstd_decompressor_new](../b/bbstreamer_zstd_decompressor_new.md) (creates zstd decompressor)
  - [bbstreamer_recovery_injector_new](../b/bbstreamer_recovery_injector_new.md) (creates recovery configuration injector)
  - [get_tablespace_mapping](../g/get_tablespace_mapping.md) (maps tablespace paths)
  - is_absolute_path (checks if path is absolute)
  - [progress_update_filename](../p/progress_update_filename.md) (updates progress reporting filename)
  - [strlcat](../s/strlcat.md) (string concatenation function)
  - pg_log_error (error logging function)
  - pg_log_error_hint (error hint logging function)
  - pg_log_error_detail (error detail logging function)
- Called from (representative examples):
  - CompressionLocation
  - [ReceiveArchiveStreamChunk](../R/ReceiveArchiveStreamChunk.md)
  - [ReceiveTarFile](../R/ReceiveTarFile.md)

## Notes and Other Information
- This is a static function with internal linkage within pg_basebackup.c
- The function implements sophisticated logic to determine when archive parsing is necessary
- Handles validation and error reporting for incompatible option combinations (e.g., manifest injection into compressed tar files)
- Supports multiple compression algorithms with appropriate file extension handling
- The returned bbstreamer represents the head of a processing pipeline that may include multiple chained components
- File extension detection is used to infer compression types (.tar.gz, .tar.lz4, .tar.zst)
- Critical for pg_basebackup functionality as it determines how backup data will be processed and output
- The function carefully manages the order of streaming components to ensure correct data transformation
- Includes extensive error checking and user-friendly error messages for invalid configurations