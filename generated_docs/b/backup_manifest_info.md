# backup_manifest_info

## Location
src/include/backup/backup_manifest.h: 27 - 36

## Overview
The backup_manifest_info structure maintains state information for generating a backup manifest during PostgreSQL base backup operations.

## Definition


## Detailed Description
The backup_manifest_info structure serves as the primary state container for backup manifest generation during PostgreSQL base backup operations. This structure tracks all necessary information to create a comprehensive manifest file that documents the contents and integrity of a backup. The manifest includes metadata about all files in the backup, their checksums, and various backup parameters. When a backup manifest is disabled (MANIFEST_OPTION_NO), the buffile field is set to NULL to indicate no manifest processing should occur.

## Parameters / Member Variables
- : Temporary buffer file used to store the manifest content as it's being generated; NULL if manifest is disabled
- : The checksum algorithm type used for data file verification within the backup
- : Cryptographic hash context for computing SHA-256 checksum of the manifest file itself (always SHA-256 regardless of data file checksum type)
- : Running count of bytes written to the manifest file
- : Boolean flag indicating whether to force base64 encoding of file paths and other content in the manifest
- : Boolean flag tracking whether the next file to be added will be the first file entry in the manifest JSON structure
- : Boolean flag indicating whether manifest checksumming is still active and updating

## Dependencies
- Functions called/Symbols referenced:
  - BufFile
  - pg_checksum_type  
  - pg_cryptohash_ctx
- Called from (representative examples):
  - InitializeBackupManifest
  - IsManifestEnabled
  - AddFileToBackupManifest
  - AddWALInfoToBackupManifest
  - SendBackupManifest
  - FreeBackupManifest
  - perform_base_backup
  - sendFileWithContent

## Notes and Other Information
- The manifest checksum always uses SHA-256 algorithm regardless of the data file checksum type configuration
- The structure is initialized to zeros before use by InitializeBackupManifest
- When manifest generation is disabled, only the buffile field needs to be NULL - other fields may contain default values
- The force_encode option is useful for ensuring manifest content can be safely transmitted over text-based protocols
- The manifest follows JSON format with version 2 specification including system identifier and file listing