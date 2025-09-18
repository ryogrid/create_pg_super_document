# parse_manifest_file

## Location
src/bin/pg_verifybackup/pg_verifybackup.c: 390 - 506

## Overview
Parses a PostgreSQL backup manifest file and returns a data structure containing the parsed manifest information including file metadata and validation callbacks.

## Definition


## Detailed Description
The parse_manifest_file function is responsible for reading and parsing a PostgreSQL backup manifest file. It opens the specified manifest file, determines its size, and creates a hash table to store the manifest data. The function supports both single-chunk reading for smaller files and incremental parsing for larger files to handle memory efficiently.

The function sets up a JsonManifestParseContext with appropriate callback functions for handling different parts of the manifest (version, system identifier, per-file data, WAL ranges, and errors). For large files, it uses chunked reading with intelligent chunk sizing to ensure the final chunk contains the complete checksum information.

## Parameters / Member Variables
- : Path to the manifest file to be parsed

## Dependencies
- Functions called/Symbols referenced:
  - open (open the manifest file)
  - report_fatal_error (error reporting for file operations)
  - fstat (get file statistics)
  - manifest_files_create (create hash table for manifest files)
  - pg_malloc0 (allocate and zero-initialize memory for result)
  - verifybackup_version_cb (callback for version information)
  - verifybackup_system_identifier (callback for system identifier)
  - verifybackup_per_file_cb (callback for per-file data)
  - verifybackup_per_wal_range_cb (callback for WAL range data)
  - report_manifest_error (callback for parsing errors)
  - pg_malloc (allocate memory for buffer)
  - read (read file content)
  - close (close file descriptor)
  - json_parse_manifest (parse manifest in single chunk)
  - json_parse_manifest_incremental_init (initialize incremental parsing)
  - json_parse_manifest_incremental_chunk (parse manifest chunk)
  - json_parse_manifest_incremental_shutdown (cleanup incremental parsing)
  - pfree (free allocated buffer memory)
- Called from:
  - main (in src/bin/pg_verifybackup/pg_verifybackup.c:345)

## Notes and Other Information
- Located in src/bin/pg_verifybackup/pg_verifybackup.c:390-506
- Uses READ_CHUNK_SIZE constant for chunked reading of large files
- Estimates hash table size based on ESTIMATED_BYTES_PER_MANIFEST_LINE
- Implements intelligent chunking strategy to ensure final chunk is at least half the chunk size to contain complete checksum data
- Returns a dynamically allocated manifest_data structure that must be freed by the caller
- Handles both O(1) single-read parsing for small files and streaming incremental parsing for large files
- Uses PG_BINARY flag for cross-platform compatibility when opening files