# AddWALInfoToBackupManifest

## Location
src/backend/backup/backup_manifest.c: 212 - 315

## Overview
Adds Write-Ahead Log (WAL) range information to the backup manifest by determining which WAL segments and timeline ranges are necessary for backup recovery.

## Definition
```c
void AddWALInfoToBackupManifest(backup_manifest_info *manifest, XLogRecPtr startptr,
                               TimeLineID starttli, XLogRecPtr endptr,
                               TimeLineID endtli)
```

## Detailed Description
AddWALInfoToBackupManifest concludes the file section of the backup manifest and adds essential WAL recovery information by analyzing timeline history and determining the exact WAL ranges needed for backup restoration. The function reads the timeline history for the ending timeline and iterates through relevant timeline entries to construct WAL-Ranges JSON objects. It validates timeline consistency, handles timeline transitions, and ensures that all necessary WAL segments from backup start to backup end are properly recorded. The function also performs critical validation to ensure the starting timeline exists in the ending timeline's history, preventing restoration issues.

## Parameters / Member Variables
- `manifest`: Pointer to backup_manifest_info structure for the active backup manifest
- `startptr`: Log sequence number (LSN) where the backup began
- `starttli`: Timeline identifier on which the backup started
- `endptr`: Log sequence number (LSN) where the backup ended
- `endtli`: Timeline identifier on which the backup ended

## Dependencies
- Functions called/Symbols referenced:
  - [IsManifestEnabled](../I/IsManifestEnabled.md) (manifest enablement check)
  - [AppendStringToManifest](AppendStringToManifest.md) (internal manifest writing)
  - [readTimeLineHistory](../r/readTimeLineHistory.md) (timeline history parsing - from related processed symbols)
  - AppendToManifest (formatted manifest writing)
  - XLogRecPtrIsInvalid (PostgreSQL WAL pointer validation)
  - TimeLineHistoryEntry (timeline history structure)
  - ereport, errmsg (PostgreSQL error reporting)
  - LSN_FORMAT_ARGS (WAL LSN formatting macro)
- Called from (representative examples):
  - [perform_base_backup](../p/perform_base_backup.md) (src/backend/backup/basebackup.c:645)

## Notes and Other Information
- Returns early if manifest generation is disabled via IsManifestEnabled check
- Terminates the Files JSON array before starting the WAL-Ranges section
- Timeline history is processed in reverse chronological order (newer timelines first)
- Validates that the first relevant timeline matches the backup's ending timeline
- Handles timeline branch points by using timeline begin LSNs for newer timelines
- Performs critical validation that the starting timeline exists in the ending timeline's history
- Constructs WAL ranges that cover all necessary segments for point-in-time recovery
- Each WAL range specifies Timeline, Start-LSN, and End-LSN in hexadecimal format
- Essential for ensuring backup restoration can replay all required WAL segments
- Works with PostgreSQL's timeline branching system used for point-in-time recovery scenarios