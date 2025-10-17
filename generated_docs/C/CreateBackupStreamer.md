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

## Simplified Source

```c
static bbstreamer *
CreateBackupStreamer(char *archive_name, char *spclocation,
                     bbstreamer **manifest_inject_streamer_p,
                     bool is_recovery_guc_supported, bool expect_unterminated_tarfile,
                     pg_compress_specification *compress)
{
    bbstreamer *streamer = NULL;
    bbstreamer *manifest_inject_streamer = NULL;
    bool inject_manifest, is_tar, is_compressed_tar, must_parse_archive;
    int archive_name_len = strlen(archive_name);

    // Determine if we need to inject manifest (only for tar to stdout with manifest enabled)
    inject_manifest = (format == 't' && strcmp(basedir, "-") == 0 && manifest);

    // Detect archive types based on file extension
    is_tar = (archive_name_len > 4 && strcmp(archive_name + archive_name_len - 4, ".tar") == 0);
    bool is_tar_gz = (archive_name_len > 7 && strcmp(archive_name + archive_name_len - 7, ".tar.gz") == 0);
    bool is_tar_lz4 = (archive_name_len > 8 && strcmp(archive_name + archive_name_len - 8, ".tar.lz4") == 0);
    bool is_tar_zstd = (archive_name_len > 8 && strcmp(archive_name + archive_name_len - 8, ".tar.zst") == 0);
    is_compressed_tar = is_tar_gz || is_tar_lz4 || is_tar_zstd;

    // Validate manifest injection compatibility
    if (inject_manifest && is_compressed_tar) {
        pg_log_error("cannot inject manifest into a compressed tar file");
        pg_log_error_hint("Use client-side compression, send output to directory, or use --no-manifest");
        exit(1);
    }

    // Determine if we need to parse the archive
    must_parse_archive = (format == 'p' || inject_manifest || (spclocation == NULL && writerecoveryconf));

    // Validate that we can parse this archive type
    if (must_parse_archive && !is_tar && !is_compressed_tar) {
        pg_log_error("cannot parse archive \"%s\"", archive_name);
        pg_log_error_detail("Only tar archives can be parsed.");
        exit(1);
    }

    if (format == 'p') {
        // Plain format - extract to directory
        const char *directory;
        if (spclocation == NULL)
            directory = basedir;
        else if (!is_absolute_path(spclocation))
            directory = psprintf("%s/%s", basedir, spclocation);
        else
            directory = get_tablespace_mapping(spclocation);

        streamer = bbstreamer_extractor_new(directory, get_tablespace_mapping, progress_update_filename);
    }
    else {
        // Tar format - create appropriate writer with compression
        char archive_filename[MAXPGPATH];
        FILE *archive_file = NULL;

        if (strcmp(basedir, "-") == 0) {
            snprintf(archive_filename, sizeof(archive_filename), "-");
            archive_file = stdout;
        }
        else {
            snprintf(archive_filename, sizeof(archive_filename), "%s/%s", basedir, archive_name);
        }

        // Create writer based on compression algorithm
        if (compress->algorithm == PG_COMPRESSION_NONE) {
            streamer = bbstreamer_plain_writer_new(archive_filename, archive_file);
        }
        else if (compress->algorithm == PG_COMPRESSION_GZIP) {
            strlcat(archive_filename, ".gz", sizeof(archive_filename));
            streamer = bbstreamer_gzip_writer_new(archive_filename, archive_file, compress);
        }
        else if (compress->algorithm == PG_COMPRESSION_LZ4) {
            strlcat(archive_filename, ".lz4", sizeof(archive_filename));
            streamer = bbstreamer_plain_writer_new(archive_filename, archive_file);
            streamer = bbstreamer_lz4_compressor_new(streamer, compress);
        }
        else if (compress->algorithm == PG_COMPRESSION_ZSTD) {
            strlcat(archive_filename, ".zst", sizeof(archive_filename));
            streamer = bbstreamer_plain_writer_new(archive_filename, archive_file);
            streamer = bbstreamer_zstd_compressor_new(streamer, compress);
        }

        // Add tar archiver if we need to parse
        if (must_parse_archive)
            streamer = bbstreamer_tar_archiver_new(streamer);

        progress_update_filename(archive_filename);
    }

    // Set up manifest injection point
    if (inject_manifest)
        manifest_inject_streamer = streamer;

    // Add recovery configuration injection for main tablespace
    if (spclocation == NULL && writerecoveryconf) {
        streamer = bbstreamer_recovery_injector_new(streamer, is_recovery_guc_supported, recoveryconfcontents);
    }

    // Add tar parser or terminator as needed
    if (must_parse_archive)
        streamer = bbstreamer_tar_parser_new(streamer);
    else if (expect_unterminated_tarfile)
        streamer = bbstreamer_tar_terminator_new(streamer);

    // Add decompression for plain format with compressed input
    if (format == 'p') {
        if (is_tar_gz)
            streamer = bbstreamer_gzip_decompressor_new(streamer);
        else if (is_tar_lz4)
            streamer = bbstreamer_lz4_decompressor_new(streamer);
        else if (is_tar_zstd)
            streamer = bbstreamer_zstd_decompressor_new(streamer);
    }

    *manifest_inject_streamer_p = manifest_inject_streamer;
    return streamer;
}
```