# btree_redo

## Location
[src/backend/access/nbtree/nbtxlog.c:1014-1072](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtxlog.c#L1014-L1072)

## Overview
Main entry point for B-tree WAL record replay, dispatching different B-tree operation types to their respective recovery handlers.

## Definition
```c
void btree_redo(XLogReaderState *record)
```

## Detailed Description
This function serves as the central dispatcher for all B-tree Write-Ahead Log (WAL) record replay operations during database recovery. When the recovery process encounters a B-tree-related WAL record, it calls this function to determine the specific operation type and delegate to the appropriate specialized recovery handler.

The function performs several key operations:
1. Extracts the operation type (info) from the WAL record header
2. Switches to a dedicated memory context for B-tree recovery operations
3. Dispatches to the appropriate recovery handler based on the operation type
4. Restores the previous memory context and resets the recovery context

The function handles all major B-tree operations including:
- Page insertions (leaf, upper, meta, post-split)
- Page splits (left and right)
- Page deduplication and vacuum operations
- Page deletion and unlinking
- Root page creation
- Page reuse and meta cleanup

Each operation type has its own specialized recovery function that reconstructs the B-tree changes from the WAL record data.

## Parameters / Member Variables
- `record`: XLogReaderState containing the WAL record data, operation type, and block references

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetInfo
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [MemoryContextReset](../M/MemoryContextReset.md)
  - [btree_xlog_insert](btree_xlog_insert.md)
  - [btree_xlog_split](btree_xlog_split.md)
  - [btree_xlog_dedup](btree_xlog_dedup.md)
  - [btree_xlog_vacuum](btree_xlog_vacuum.md)
  - [btree_xlog_delete](btree_xlog_delete.md)
  - [btree_xlog_mark_page_halfdead](btree_xlog_mark_page_halfdead.md)
  - [btree_xlog_unlink_page](btree_xlog_unlink_page.md)
  - [btree_xlog_newroot](btree_xlog_newroot.md)
  - [btree_xlog_reuse_page](btree_xlog_reuse_page.md)
  - [_bt_restore_meta](_bt_restore_meta.md)
  - elog (PANIC)
- Called from (representative examples):
  - PostgreSQL WAL recovery system

## Notes and Other Information
- Uses a dedicated memory context (opCtx) for recovery operations to prevent memory leaks
- The info byte is masked with ~XLR_INFO_MASK to extract the pure operation type
- Supports both regular and meta-updating variants of unlink operations
- Panics with an error if an unknown operation code is encountered
- The memory context is reset after each operation to clean up temporary allocations
- This function is registered as the redo handler for B-tree access method in PostgreSQL's WAL system
- Operation types include: INSERT_LEAF, INSERT_UPPER, INSERT_META, SPLIT_L, SPLIT_R, INSERT_POST, DEDUP, VACUUM, DELETE, MARK_PAGE_HALFDEAD, UNLINK_PAGE, NEWROOT, REUSE_PAGE, META_CLEANUP

## Simplified Source

```c
void
btree_redo(XLogReaderState *record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    MemoryContext oldCtx;

    // Switch to dedicated recovery memory context
    oldCtx = MemoryContextSwitchTo(opCtx);

    // Dispatch to appropriate recovery handler
    switch (info)
    {
        case XLOG_BTREE_INSERT_LEAF:
            btree_xlog_insert(true, false, false, record);
            break;
        case XLOG_BTREE_INSERT_UPPER:
            btree_xlog_insert(false, false, false, record);
            break;
        case XLOG_BTREE_INSERT_META:
            btree_xlog_insert(false, true, false, record);
            break;
        case XLOG_BTREE_SPLIT_L:
            btree_xlog_split(true, record);
            break;
        case XLOG_BTREE_SPLIT_R:
            btree_xlog_split(false, record);
            break;
        case XLOG_BTREE_INSERT_POST:
            btree_xlog_insert(true, false, true, record);
            break;
        case XLOG_BTREE_DEDUP:
            btree_xlog_dedup(record);
            break;
        case XLOG_BTREE_VACUUM:
            btree_xlog_vacuum(record);
            break;
        case XLOG_BTREE_DELETE:
            btree_xlog_delete(record);
            break;
        case XLOG_BTREE_MARK_PAGE_HALFDEAD:
            btree_xlog_mark_page_halfdead(info, record);
            break;
        case XLOG_BTREE_UNLINK_PAGE:
        case XLOG_BTREE_UNLINK_PAGE_META:
            btree_xlog_unlink_page(info, record);
            break;
        case XLOG_BTREE_NEWROOT:
            btree_xlog_newroot(record);
            break;
        case XLOG_BTREE_REUSE_PAGE:
            btree_xlog_reuse_page(record);
            break;
        case XLOG_BTREE_META_CLEANUP:
            _bt_restore_meta(record, 0);
            break;
        default:
            elog(PANIC, "btree_redo: unknown op code %u", info);
    }

    // Restore previous context and clean up
    MemoryContextSwitchTo(oldCtx);
    MemoryContextReset(opCtx);
}
```