# AppendStringToManifest

## Location
[src/backend/backup/backup_manifest.c:383-396](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/backup_manifest.c#L383-L396)

## Overview
Appends a C string to the backup manifest while updating the running checksum if still in the checksumming phase.

## Definition
```c
static void AppendStringToManifest(backup_manifest_info *manifest, const char *s)
```

## Detailed Description
`AppendStringToManifest` is a utility function that handles writing string data to the backup manifest file. It performs two key operations:

1. **Checksum Updates**: If the manifest is still in the checksumming phase (`manifest->still_checksumming` is true), it updates the running SHA-256 checksum with the new string data using `pg_cryptohash_update`.

2. **File Writing**: Writes the string data to the manifest buffer file using `BufFileWrite` and updates the total manifest size counter.

This function is designed to be called throughout the manifest generation process to build up the JSON manifest content incrementally while maintaining an accurate checksum of the entire manifest.

## Parameters / Member Variables
- `manifest`: Pointer to backup_manifest_info structure containing the manifest state, buffer file, checksum context, and size tracking
- `s`: Null-terminated C string to append to the manifest

## Dependencies
- Functions called/Symbols referenced:
  - strlen (implicit)
  - Assert (for input validation)
  - [pg_cryptohash_update](../p/pg_cryptohash_update.md)
  - [pg_cryptohash_error](../p/pg_cryptohash_error.md)
  - BufFileWrite
- Called from (representative examples):
  - AppendToManifest (src/backend/backup/backup_manifest.c:44)
  - [AddFileToBackupManifest](AddFileToBackupManifest.md) (src/backend/backup/backup_manifest.c:201)
  - [AddWALInfoToBackupManifest](AddWALInfoToBackupManifest.md) (src/backend/backup/backup_manifest.c:225, 231, 309)
  - [SendBackupManifest](../S/SendBackupManifest.md) (src/backend/backup/backup_manifest.c:341, 346, 347)

## Notes and Other Information
- This is a static function, meaning it is only accessible within the backup_manifest.c file
- The function includes an assertion to ensure the manifest parameter is not NULL
- Checksum updates only occur when `still_checksumming` is true; this flag is set to false in `SendBackupManifest` before finalizing the checksum
- Error handling includes specific error messages for checksum update failures
- The function automatically tracks the total manifest size by incrementing `manifest->manifest_size` by the length of each appended string
- Used extensively throughout the manifest generation process to build JSON content incrementally