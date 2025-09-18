# verifybackup_per_file_cb

## Location
src/bin/pg_verifybackup/pg_verifybackup.c: 548 - 576

## Overview
Records details extracted from the backup manifest for one file and stores them in a hash table for later verification during the backup verification process.

## Definition


## Detailed Description
This function serves as a callback during backup manifest parsing. When the JSON manifest parser encounters a file entry, it calls this function to record the file's metadata (path, size, checksum information) in a hash table structure. The function creates a new manifest_file entry in the hash table, initializes it with the provided metadata, and sets up tracking flags (matched and bad) that will be used during the actual verification process. If a duplicate pathname is found in the manifest, the function reports a fatal error.

## Parameters / Member Variables
- : Parsing context containing private data with the manifest hash table
- : The file path as specified in the backup manifest
- : Expected size of the file in bytes
- : Type of checksum algorithm used (e.g., CRC32C, SHA256)
- : Length of the checksum data in bytes
- : Binary checksum data for the file

## Dependencies
- Functions called/Symbols referenced:
  - manifest_files_insert
  - report_fatal_error
- Types referenced:
  - JsonManifestParseContext
  - pg_checksum_type
  - manifest_data
  - manifest_file
- Called from (representative examples):
  - parse_manifest_file

## Notes and Other Information
- This is a static callback function specifically designed for use with the JSON manifest parser
- The function ensures no duplicate pathnames exist in the manifest by reporting fatal errors
- The matched and bad flags are initialized to false and will be updated during verification
- The checksum_payload memory is assumed to be managed by the caller
- This function is part of the pg_verifybackup utility's manifest processing pipeline