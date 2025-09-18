# InitializeBackupManifest

## Location
src/backend/backup/backup_manifest.c: 56 - 90

## Overview
Initializes the backup manifest infrastructure by setting up the manifest buffer file, cryptographic checksum context, and writing the initial JSON header for backup manifest generation.

## Definition
```c
void InitializeBackupManifest(backup_manifest_info *manifest,
                             backup_manifest_option want_manifest,
                             pg_checksum_type manifest_checksum_type)
```

## Detailed Description
InitializeBackupManifest prepares the backup manifest system for operation by clearing the manifest structure, configuring the desired checksum type for data files, and conditionally setting up manifest generation based on user preferences. When manifest generation is enabled, it creates a temporary buffer file, initializes a SHA-256 cryptographic context for the manifest's own checksum (regardless of the data file checksum type), and writes the JSON header containing the PostgreSQL backup manifest version and system identifier. The function supports three manifest modes: disabled, normal, and force-encode (for testing special character handling).

## Parameters / Member Variables
- `manifest`: Pointer to backup_manifest_info structure to be initialized
- `want_manifest`: Enumeration value specifying the desired manifest behavior (MANIFEST_OPTION_NO, normal, or MANIFEST_OPTION_FORCE_ENCODE)
- `manifest_checksum_type`: Checksum algorithm to use for data files in the backup

## Dependencies
- Functions called/Symbols referenced:
  - memset (C standard library)
  - BufFileCreateTemp (PostgreSQL buffer file management)
  - pg_cryptohash_create (PostgreSQL cryptographic hash functions)
  - pg_cryptohash_init
  - pg_cryptohash_error
  - AppendToManifest (internal manifest writing function)
  - GetSystemIdentifier (PostgreSQL system identification)
  - backup_manifest_option (enum type)
  - pg_checksum_type (enum type)
  - PG_SHA256 (cryptographic constant)
- Called from (representative examples):
  - perform_base_backup (src/backend/backup/basebackup.c:257)

## Notes and Other Information
- The manifest's own checksum always uses SHA-256, regardless of the checksum type specified for data files
- Creates a temporary buffer file only when manifest generation is requested
- Initializes the JSON structure with version 2 format and includes the database system identifier
- The force_encode option is primarily used for testing scenarios involving special character encoding
- Sets up initial state flags like first_file and still_checksumming to control subsequent manifest operations