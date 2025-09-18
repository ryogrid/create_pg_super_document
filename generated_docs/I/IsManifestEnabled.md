# IsManifestEnabled

## Location
src/backend/backup/backup_manifest.c: 33 - 40

## Overview
Determines whether backup manifest generation is enabled for a given backup operation by checking if the manifest buffer file is initialized.

## Definition


## Detailed Description
IsManifestEnabled is a utility function that provides a simple boolean check to determine if backup manifest functionality should be active. The function follows a design philosophy where the manifest_info object is always present to avoid excessive NULL pointer checks throughout the codebase, but the actual manifest generation is controlled by whether the buffile member is NULL or not. When manifest->buffile is NULL, it indicates that the user has disabled manifest generation; when it's non-NULL, manifest generation is enabled.

## Parameters / Member Variables
- USAGE:
  /usr/bin/manifest export [-|URL|FILENAME]
  /usr/bin/manifest import -|URL|FILENAME: Pointer to backup_manifest_info structure containing the manifest state and buffer file information

## Dependencies
- Functions called/Symbols referenced:
  - backup_manifest_info (structure type)
- Called from (representative examples):
  - AddFileToBackupManifest (src/backend/backup/backup_manifest.c:109)
  - AddWALInfoToBackupManifest (src/backend/backup/backup_manifest.c:221)  
  - SendBackupManifest (src/backend/backup/backup_manifest.c:322)

## Notes and Other Information
- This is a static inline function, meaning it's only accessible within the backup_manifest.c file and will be inlined by the compiler for performance
- The function serves as a clean abstraction layer that encapsulates the logic for determining manifest enablement
- By always maintaining a manifest_info object regardless of user preference, the code avoids scattered NULL checks and maintains cleaner control flow