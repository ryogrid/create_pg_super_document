# combinebackup_per_wal_range_cb

## Location
src/bin/pg_combinebackup/load_manifest.c: 293 - 314

## Overview
A callback function used during backup manifest parsing that records details extracted from the backup manifest for one WAL range and adds it to a linked list of WAL ranges.

## Definition
```c
static void combinebackup_per_wal_range_cb(JsonManifestParseContext *context,
                                          TimeLineID tli,
                                          XLogRecPtr start_lsn, XLogRecPtr end_lsn)
```

## Detailed Description
This function serves as a callback for the JSON manifest parsing subsystem in pg_combinebackup. When the parser encounters a WAL range entry in the backup manifest, it invokes this callback to process and store the information. The function creates a new `manifest_wal_range` structure, populates it with the provided WAL range details, and appends it to a doubly-linked list maintained in the manifest data structure. This allows pg_combinebackup to keep track of all WAL ranges described in the backup manifest for later processing.

The function maintains the linked list by properly setting both forward and backward pointers, ensuring the list remains traversable in both directions. It handles both the case where this is the first WAL range (initializing the list) and subsequent ranges (appending to the existing list).

## Parameters
- `context`: JsonManifestParseContext pointer containing parsing state and private data (manifest_data structure)
- `tli`: TimeLineID indicating which timeline this WAL range belongs to
- `start_lsn`: XLogRecPtr specifying the starting Log Sequence Number of this WAL range
- `end_lsn`: XLogRecPtr specifying the ending Log Sequence Number of this WAL range

## Dependencies
- Functions called/Symbols referenced:
  - palloc (memory allocation)
  - JsonManifestParseContext (parsing context structure)
  - manifest_data (private data structure containing WAL range list)
  - manifest_wal_range (WAL range data structure)
  - TimeLineID (timeline identifier type)
  - XLogRecPtr (WAL position type)

- Called from:
  - load_backup_manifest (via callback registration in SH_DEFINE at src/bin/pg_combinebackup/load_manifest.c:148)

## Notes and Other Information
- This is a static function, only accessible within the load_manifest.c compilation unit
- Uses palloc for memory allocation, which provides error handling and memory context management
- Maintains a doubly-linked list structure for efficient traversal in both directions
- Part of the pg_combinebackup utility's manifest parsing infrastructure
- The function assumes the context's private_data field points to a valid manifest_data structure
- Memory allocated by palloc will be automatically freed when the memory context is reset or deleted