# load_backup_manifest

## Location
src/bin/pg_combinebackup/load_manifest.c: 105 - 227

## Overview
Parses the backup_manifest file in the named backup directory and constructs a hash table with information about all the files it mentions, along with a linked list of all the WAL ranges it mentions.

## Definition


## Detailed Description
This function loads and parses a PostgreSQL backup manifest file ("backup_manifest") located in the specified backup directory. The manifest contains metadata about all files in the backup and WAL ranges. The function creates a hash table to efficiently store file information and initializes callback functions for parsing different sections of the JSON manifest.

The function handles both small manifests (loaded entirely into memory) and large manifests (parsed incrementally in chunks) to efficiently manage memory usage. If the backup_manifest file doesn't exist, it logs a warning and returns NULL rather than failing fatally.

## Parameters / Member Variables
- `backup_directory`: The directory path containing the backup_manifest file to be loaded and parsed

## Dependencies
- Functions called/Symbols referenced:
  - open, fstat, read, close (system calls)
  - manifest_files_create
  - pg_malloc0, pg_malloc, pfree
  - json_parse_manifest
  - json_parse_manifest_incremental_init
  - json_parse_manifest_incremental_chunk  
  - json_parse_manifest_incremental_shutdown
  - combinebackup_version_cb
  - combinebackup_system_identifier_cb
  - combinebackup_per_file_cb
  - combinebackup_per_wal_range_cb
  - report_manifest_error
- Called from:
  - load_backup_manifests (src/bin/pg_combinebackup/load_manifest.c:90)
  - main function in pg_combinebackup via load_backup_manifests

## Notes and Other Information
- Returns NULL if the backup_manifest file doesn't exist (logs warning)
- Estimates initial hash table size based on manifest file size using ESTIMATED_BYTES_PER_MANIFEST_LINE
- Uses READ_CHUNK_SIZE for incremental parsing of large manifest files
- Handles chunked reading intelligently to ensure the last chunk contains the complete checksum portion
- Sets up comprehensive callback functions for different JSON manifest elements during parsing
- Memory management includes proper cleanup of buffers and incremental parsing state