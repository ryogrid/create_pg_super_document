# hash_desc

## Location
[src/backend/access/rmgrdesc/hashdesc.c:20-125](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/rmgrdesc/hashdesc.c#L20-L125)

## Overview
The hash_desc function provides detailed descriptions of hash index WAL (Write-Ahead Log) records for debugging and logging purposes.

## Definition
void hash_desc(StringInfo buf, XLogReaderState *record)

## Detailed Description
This function decodes and formats various hash index-related WAL record types into human-readable descriptions. It is part of PostgreSQL's WAL record description framework, allowing administrators and developers to understand the contents of hash index operations recorded in the transaction log. The function examines the record type and extracts relevant information from the WAL record data, appending formatted descriptions to a string buffer.

The function handles multiple hash index operations including metadata initialization, bitmap page operations, tuple insertions, bucket splitting, page movement, and vacuum operations.

## Parameters / Member Variables
- `buf`: StringInfo buffer where the formatted description will be appended
- `record`: XLogReaderState pointer containing the WAL record to be described

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - XLogRecGetInfo
  - [appendStringInfo](../a/appendStringInfo.md)
- WAL record types handled:
  - XLOG_HASH_INIT_META_PAGE
  - XLOG_HASH_INIT_BITMAP_PAGE
  - XLOG_HASH_INSERT
  - XLOG_HASH_ADD_OVFL_PAGE
  - XLOG_HASH_SPLIT_ALLOCATE_PAGE
  - XLOG_HASH_SPLIT_COMPLETE
  - XLOG_HASH_MOVE_PAGE_CONTENTS
  - XLOG_HASH_SQUEEZE_PAGE
  - XLOG_HASH_DELETE
  - XLOG_HASH_UPDATE_META_PAGE
  - XLOG_HASH_VACUUM_ONE_PAGE
- Called from (representative examples):
  - SizeOfHashVacuumOnePage

## Notes and Other Information
- This function is primarily used for debugging and administrative purposes
- Each case in the switch statement corresponds to a specific hash index operation type
- The function extracts operation-specific details from the WAL record and formats them into descriptive text
- [Boolean](../B/Boolean.md) values are displayed as 'T' (true) or 'F' (false) for readability
- Part of PostgreSQL's resource manager description framework for hash indexes

## Simplified Source

```c
void hash_desc(StringInfo buf, XLogReaderState *record) {
    char *rec = XLogRecGetData(record);
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    // Process different hash index WAL record types
    switch (info) {
        case XLOG_HASH_INIT_META_PAGE:
            {
                xl_hash_init_meta_page *xlrec = (xl_hash_init_meta_page *) rec;
                appendStringInfo(buf, "num_tuples %g, fillfactor %d",
                                xlrec->num_tuples, xlrec->ffactor);
                break;
            }
        case XLOG_HASH_INIT_BITMAP_PAGE:
            {
                xl_hash_init_bitmap_page *xlrec = (xl_hash_init_bitmap_page *) rec;
                appendStringInfo(buf, "bmsize %d", xlrec->bmsize);
                break;
            }
        case XLOG_HASH_INSERT:
            {
                xl_hash_insert *xlrec = (xl_hash_insert *) rec;
                appendStringInfo(buf, "off %u", xlrec->offnum);
                break;
            }
        case XLOG_HASH_ADD_OVFL_PAGE:
            {
                xl_hash_add_ovfl_page *xlrec = (xl_hash_add_ovfl_page *) rec;
                appendStringInfo(buf, "bmsize %d, bmpage_found %c",
                                xlrec->bmsize, xlrec->bmpage_found ? 'T' : 'F');
                break;
            }
        case XLOG_HASH_SPLIT_ALLOCATE_PAGE:
            {
                xl_hash_split_allocate_page *xlrec = (xl_hash_split_allocate_page *) rec;
                appendStringInfo(buf, "new_bucket %u, meta_page_masks_updated %c, issplitpoint_changed %c",
                                xlrec->new_bucket,
                                (xlrec->flags & XLH_SPLIT_META_UPDATE_MASKS) ? 'T' : 'F',
                                (xlrec->flags & XLH_SPLIT_META_UPDATE_SPLITPOINT) ? 'T' : 'F');
                break;
            }
        // Additional cases for complete, move, squeeze, delete, update, vacuum operations
        // Each formats specific operation parameters for debugging output
    }
}
```