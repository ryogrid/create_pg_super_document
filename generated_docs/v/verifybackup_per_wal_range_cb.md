# verifybackup_per_wal_range_cb

## Location
[src/bin/pg_verifybackup/pg_verifybackup.c:577-609](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_verifybackup/pg_verifybackup.c#L577-L609)

## Overview
Records details extracted from the backup manifest for one WAL range and maintains them in a linked list for later verification during the backup verification process.

## Definition
```c
static void verifybackup_per_wal_range_cb(JsonManifestParseContext *context,
                                          TimeLineID tli,
                                          XLogRecPtr start_lsn, XLogRecPtr end_lsn)
```

## Detailed Description
This function serves as a callback during backup manifest parsing for WAL (Write-Ahead Log) range entries. When the JSON manifest parser encounters a WAL range specification, it calls this function to record the timeline ID and LSN (Log Sequence Number) range information. The function allocates a new manifest_wal_range structure, initializes it with the provided WAL range data, and appends it to a doubly-linked list maintained in the manifest data structure. This linked list preserves the order of WAL ranges as they appear in the manifest for subsequent verification operations.

## Parameters / Member Variables
- `context`: Parsing context containing private data with the manifest structure
- `tli`: Timeline ID identifying the specific timeline for this WAL range
- `start_lsn`: Starting Log Sequence Number (XLogRecPtr) for this WAL range
- `end_lsn`: Ending Log Sequence Number (XLogRecPtr) for this WAL range

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md)
- Types referenced:
  - [JsonManifestParseContext](../J/JsonManifestParseContext.md)
  - TimeLineID
  - XLogRecPtr
  - [manifest_data](../m/manifest_data.md)
  - [manifest_wal_range](../m/manifest_wal_range.md)
- Called from (representative examples):
  - [parse_manifest_file](../p/parse_manifest_file.md)

## Notes and Other Information
- This is a static callback function specifically designed for use with the JSON manifest parser
- The function maintains WAL ranges in a doubly-linked list structure for efficient traversal
- WAL ranges represent contiguous segments of transaction log data that should be present in the backup
- The linked list preserves the chronological order of WAL ranges as specified in the manifest
- Memory allocation uses palloc, which is PostgreSQL's memory management function
- This function is part of the pg_verifybackup utility's WAL verification pipeline

## Simplified Source

```c
static void
verifybackup_per_wal_range_cb(JsonManifestParseContext *context,
                              TimeLineID tli,
                              XLogRecPtr start_lsn, XLogRecPtr end_lsn)
{
    manifest_data *manifest = context->private_data;
    manifest_wal_range *range;

    // Allocate and initialize new WAL range structure
    range = palloc(sizeof(manifest_wal_range));
    range->tli = tli;
    range->start_lsn = start_lsn;
    range->end_lsn = end_lsn;
    range->prev = manifest->last_wal_range;
    range->next = NULL;

    // Add to end of linked list
    if (manifest->first_wal_range == NULL)
        manifest->first_wal_range = range;
    else
        manifest->last_wal_range->next = range;
    manifest->last_wal_range = range;
}
```