# SendBackupManifest

## Location
src/backend/backup/backup_manifest.c: 316 - 382

## Overview
Finalizes the backup manifest by computing its checksum and sends the completed manifest to the client via a backup sink.

## Definition
```c
void SendBackupManifest(backup_manifest_info *manifest, bbsink *sink)
```

## Detailed Description
`SendBackupManifest` is responsible for completing the backup manifest generation process. It performs several critical tasks:

1. **Checksum Finalization**: Computes a SHA-256 checksum of the entire manifest content to ensure integrity. This checksum is always SHA-256 regardless of the algorithm used for individual files.

2. **Manifest Completion**: Appends the computed checksum to the manifest in JSON format as "Manifest-Checksum" field, then closes the JSON structure.

3. **File Transmission**: Rewinds the manifest buffer file and streams its contents to the client through the provided backup sink in chunks.

4. **Resource Cleanup**: Closes the manifest buffer file and releases associated resources.

The function uses a streaming approach to handle potentially large manifest files efficiently, reading and sending data in buffer-sized chunks rather than loading the entire manifest into memory.

## Parameters / Member Variables
- `manifest`: Pointer to backup_manifest_info structure containing manifest state, buffer file, checksum context, and size information
- `sink`: Pointer to bbsink structure that handles the actual transmission of manifest data to the client

## Dependencies
- Functions called/Symbols referenced:
  - [IsManifestEnabled](../I/IsManifestEnabled.md)
  - [pg_cryptohash_final](../p/pg_cryptohash_final.md)
  - [pg_cryptohash_error](../p/pg_cryptohash_error.md)
  - [AppendStringToManifest](../A/AppendStringToManifest.md)
  - [hex_encode](../h/hex_encode.md)
  - BufFileSeek
  - BufFileReadExact
  - BufFileClose
  - bbsink_begin_manifest
  - bbsink_manifest_contents
  - bbsink_end_manifest
- Called from (representative examples):
  - [perform_base_backup](../p/perform_base_backup.md) (src/backend/backup/basebackup.c:648)

## Notes and Other Information
- The function returns early if manifest generation is disabled via IsManifestEnabled check
- SHA-256 is hardcoded for manifest checksums to avoid requiring clients to determine the algorithm dynamically
- Uses a streaming approach with buffered I/O to handle large manifests efficiently
- The manifest file is rewound after writing to prepare for reading and transmission
- Error handling includes specific messages for checksum finalization failures and file access issues