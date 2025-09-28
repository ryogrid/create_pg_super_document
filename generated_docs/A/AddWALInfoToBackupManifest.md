# AddWALInfoToBackupManifest

## Location
[src/backend/backup/backup_manifest.c:212-315](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/backup_manifest.c#L212-L315)

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
  - [TimeLineHistoryEntry](../T/TimeLineHistoryEntry.md) (timeline history structure)
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

## Simplified Source

```c
// Simplified version of AddWALInfoToBackupManifest
void AddWALInfoToBackupManifest(backup_manifest_info *manifest, XLogRecPtr startptr,
                               TimeLineID starttli, XLogRecPtr endptr,
                               TimeLineID endtli) {
    List *timelines;
    ListCell *lc;
    bool first_wal_range = true;
    bool found_start_timeline = false;

    // Early exit if manifest is disabled
    if (!IsManifestEnabled(manifest))
        return;

    // Terminate the files list and start WAL-Ranges section
    AppendStringToManifest(manifest, "\n],\n");
    timelines = readTimeLineHistory(endtli);
    AppendStringToManifest(manifest, "\"WAL-Ranges\": [\n");

    // Process timeline history in reverse chronological order
    foreach(lc, timelines) {
        TimeLineHistoryEntry *entry = lfirst(lc);
        XLogRecPtr tl_beginptr;

        // Skip timelines that ended before backup started
        if (!XLogRecPtrIsInvalid(entry->end) && entry->end < startptr)
            continue;

        // Validate first timeline matches ending timeline
        if (first_wal_range && endtli != entry->tli)
            ereport(ERROR,
                    errmsg("expected end timeline %u but found timeline %u",
                           starttli, entry->tli));

        // Determine WAL range start point
        if (starttli == entry->tli)
            tl_beginptr = startptr;
        else {
            tl_beginptr = entry->begin;
            if (XLogRecPtrIsInvalid(entry->begin))
                ereport(ERROR,
                        errmsg("expected start timeline %u but found timeline %u",
                               starttli, entry->tli));
        }

        // Add WAL range to manifest
        AppendToManifest(manifest,
                        "%s{ \"Timeline\": %u, \"Start-LSN\": \"%X/%X\", \"End-LSN\": \"%X/%X\" }",
                        first_wal_range ? "" : ",\n",
                        entry->tli,
                        LSN_FORMAT_ARGS(tl_beginptr),
                        LSN_FORMAT_ARGS(endptr));

        // Stop when we reach the starting timeline
        if (starttli == entry->tli) {
            found_start_timeline = true;
            break;
        }

        endptr = entry->begin;
        first_wal_range = false;
    }

    // Validate that starting timeline was found
    if (!found_start_timeline)
        ereport(ERROR,
                errmsg("start timeline %u not found in history of timeline %u",
                       starttli, endtli));

    // Terminate WAL ranges section
    AppendStringToManifest(manifest, "\n],\n");
}
```

Key simplifications made:
- Removed detailed comments while preserving complex timeline logic
- Streamlined the timeline processing loop
- Maintained all validation and error handling
- Preserved the JSON manifest generation functionality
- Kept timeline branching and WAL range calculation logic