# IsManifestEnabled

## Location
[src/backend/backup/backup_manifest.c:33-40](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/backup_manifest.c#L33-L40)

## Overview
Determines whether backup manifest generation is enabled for a given backup operation by checking if the manifest buffer file is initialized.

## Definition

```c
static inline bool
IsManifestEnabled(backup_manifest_info *manifest)
```
## Detailed Description
IsManifestEnabled is a utility function that provides a simple boolean check to determine if backup manifest functionality should be active. The function follows a design philosophy where the manifest_info object is always present to avoid excessive NULL pointer checks throughout the codebase, but the actual manifest generation is controlled by whether the buffile member is NULL or not. When manifest->buffile is NULL, it indicates that the user has disabled manifest generation; when it's non-NULL, manifest generation is enabled.

## Parameters / Member Variables
- USAGE:
  /usr/bin/manifest export [-|URL|FILENAME]
  /usr/bin/manifest import -|URL|FILENAME: Pointer to backup_manifest_info structure containing the manifest state and buffer file information

## Dependencies
- Functions called/Symbols referenced:
  - [backup_manifest_info](../b/backup_manifest_info.md) (structure type)
- Called from (representative examples):
  - [AddFileToBackupManifest](../A/AddFileToBackupManifest.md) (src/backend/backup/backup_manifest.c:109)
  - [AddWALInfoToBackupManifest](../A/AddWALInfoToBackupManifest.md) (src/backend/backup/backup_manifest.c:221)  
  - [SendBackupManifest](../S/SendBackupManifest.md) (src/backend/backup/backup_manifest.c:322)

## Notes and Other Information
- This is a static inline function, meaning it's only accessible within the backup_manifest.c file and will be inlined by the compiler for performance
- The function serves as a clean abstraction layer that encapsulates the logic for determining manifest enablement
- By always maintaining a manifest_info object regardless of user preference, the code avoids scattered NULL checks and maintains cleaner control flow

## Simplified Source

```c
// Check if backup manifest generation is enabled
static inline bool IsManifestEnabled(backup_manifest_info *manifest)
{
    // Manifest is enabled if buffile is not NULL
    return (manifest->buffile != NULL);
}
```