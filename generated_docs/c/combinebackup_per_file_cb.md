# combinebackup_per_file_cb

## Location
src/bin/pg_combinebackup/load_manifest.c: 268 - 292

## Overview
A callback function that processes individual file entries from backup manifests, storing file metadata (path, size, checksum information) in a hash table for pg_combinebackup operations.

## Definition


## Detailed Description
This function serves as a per-file callback during JSON manifest parsing, processing each file entry found in the backup manifest. It creates a new entry in the manifest's hash table for efficient file lookup and stores all relevant file metadata including the pathname, file size, checksum type, and checksum data.

The function enforces uniqueness by checking for duplicate pathnames and terminating with a fatal error if a duplicate is encountered. Each file entry becomes a manifest_file structure containing all necessary information for subsequent backup validation and combination operations.

## Parameters / Member Variables
- `context`: Pointer to the JSON manifest parse context containing private_data with manifest_data structure
- `pathname`: The file path as stored in the backup manifest
- `size`: The file size in bytes
- `checksum_type`: The type of checksum used for this file (enum pg_checksum_type)
- `checksum_length`: The length of the checksum data in bytes
- `checksum_payload`: Pointer to the actual checksum data bytes

## Dependencies
- Functions called/Symbols referenced:
  - manifest_files_insert
  - pg_fatal (when duplicate pathname is found)
  - JsonManifestParseContext (type reference)
  - manifest_data (type reference)
  - manifest_file (type reference)
  - pg_checksum_type (enum type)
- Called from:
  - load_backup_manifest (src/bin/pg_combinebackup/load_manifest.c:147) - set as per_file_cb callback
  - Referenced in SH_DEFINE macro context

## Notes and Other Information
- Function is declared static, limiting scope to load_manifest.c
- Enforces pathname uniqueness within a single backup manifest - duplicate paths cause fatal errors
- Stores checksum payload pointer directly without copying - relies on parser memory management
- Critical component for building the file inventory used in backup combination operations
- Designed as a callback function for the JSON manifest parser infrastructure
- Creates manifest_file entries that are later used for file validation and reconstruction
- Part of the manifest parsing callback system that builds the comprehensive file database for pg_combinebackup