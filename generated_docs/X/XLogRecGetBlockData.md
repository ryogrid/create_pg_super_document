# XLogRecGetBlockData

## Location
[src/backend/access/transam/xlogreader.c:2035-2065](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogreader.c#L2035-L2065)

## Overview
Retrieves the data associated with a specific block reference in an XLog record, returning NULL if no data is available (e.g., when a full-page image was stored instead).

## Definition
```c
char *XLogRecGetBlockData(XLogReaderState *record, uint8 block_id, Size *len)
```

## Detailed Description
XLogRecGetBlockData is a utility function for accessing block-specific data stored within WAL (Write-Ahead Log) records. It operates on decoded block backup information and provides access to the actual data payload associated with a particular block reference. The function performs validation checks to ensure the requested block ID is valid and contains data before returning a pointer to the MAXALIGNed buffer containing the block data.

The function returns NULL in two scenarios: when the block ID is invalid (exceeds max_block_id or is not in use), or when the block has no associated data (typically because a full-page image was stored instead of incremental changes). When data is available, it returns a pointer to the data buffer and optionally sets the length parameter.

## Parameters / Member Variables
- `record`: Pointer to XLogReaderState containing the decoded WAL record
- `block_id`: Identifier of the block whose data is being requested (0-based index)
- `len`: Optional output parameter that receives the length of the returned data (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - DecodedBkpBlock (struct type used for accessing block information)
- Called from (representative examples):
  - [brin_xlog_insert_update](../b/brin_xlog_insert_update.md)
  - [ginRedoInsert](../g/ginRedoInsert.md)
  - [heap_xlog_insert](../h/heap_xlog_insert.md)
  - [btree_xlog_insert](../b/btree_xlog_insert.md)
  - [generic_redo](../g/generic_redo.md)
  - [DecodeInsert](../D/DecodeInsert.md)
  - XLogRecHasBlockData

## Notes and Other Information
- The returned pointer points to a MAXALIGNed buffer, ensuring proper memory alignment
- This function is widely used across PostgreSQL access methods for WAL replay operations
- It is a key component in the WAL decoding infrastructure, enabling various subsystems to access block-specific data during recovery and logical replication
- The function handles both cases where block data exists and where only full-page images are stored

## Simplified Source

```c
char *
XLogRecGetBlockData(XLogReaderState *record, uint8 block_id, Size *len)
{
    DecodedBkpBlock *bkpb;

    // Validate block ID and ensure block is in use
    if (block_id > record->record->max_block_id ||
        !record->record->blocks[block_id].in_use)
        return NULL;

    bkpb = &record->record->blocks[block_id];

    // Check if block has associated data
    if (!bkpb->has_data)
    {
        if (len)
            *len = 0;
        return NULL;
    }
    else
    {
        // Return data pointer and length
        if (len)
            *len = bkpb->data_len;
        return bkpb->data;
    }
}
```