# FreeBackupManifest

## Location
[src/backend/backup/backup_manifest.c:91-100](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/backup_manifest.c#L91-L100)

## Overview
Releases the cryptographic hash context resources allocated during backup manifest generation to prevent memory leaks.

## Definition
```c
void FreeBackupManifest(backup_manifest_info *manifest)
```

## Detailed Description
FreeBackupManifest is a cleanup function that properly releases the cryptographic hash context that was allocated during InitializeBackupManifest. The function specifically frees the SHA-256 context used for computing the manifest's own checksum and sets the context pointer to NULL to prevent potential double-free errors or use-after-free vulnerabilities. Note that this function only handles the cryptographic context cleanup - the buffer file cleanup is handled elsewhere in the backup process.

## Parameters / Member Variables
- `manifest`: Pointer to backup_manifest_info structure containing the cryptographic context to be freed

## Dependencies
- Functions called/Symbols referenced:
  - [pg_cryptohash_free](../p/pg_cryptohash_free.md) (PostgreSQL cryptographic hash functions)
  - [backup_manifest_info](../b/backup_manifest_info.md) (structure type)
- Called from (representative examples):
  - [perform_base_backup](../p/perform_base_backup.md) (src/backend/backup/basebackup.c:671)

## Notes and Other Information
- This function should be called after backup manifest generation is complete to avoid memory leaks
- Only frees the cryptographic hash context, not the buffer file (which is handled by other cleanup mechanisms)
- Sets the context pointer to NULL after freeing to prevent accidental reuse
- Essential for proper resource management in long-running backup operations
- Part of the standard initialization/cleanup pattern used throughout PostgreSQL