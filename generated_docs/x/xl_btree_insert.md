# xl_btree_insert

## Location
src/include/access/nbtxlog.h: 79 - 85

## Overview
The xl_btree_insert structure represents a WAL record for simple B-tree insert operations that do not involve page splits.

## Definition


## Detailed Description
This structure is used to log simple insertions into B-tree pages during Write-Ahead Logging. It supports four different types of insert operations: INSERT_LEAF, INSERT_UPPER, INSERT_META, and INSERT_POST. The structure is designed to be minimal while providing enough information for crash recovery to replay the insertion operation.

For INSERT_POST operations (posting list splits), additional data follows the structure including the split offset and the new tuple data. The WAL record format includes backup blocks for the original page and potentially for child left sibling (INSERT_UPPER/INSERT_META) and metadata (INSERT_META only).

## Parameters / Member Variables
- : The offset number on the page where the new tuple should be inserted

## Dependencies
- Functions called/Symbols referenced:
  - OffsetNumber (type)

- Called from (representative examples):
  - _bt_insertonpg (src/backend/access/nbtree/nbtinsert.c:1314)
  - btree_xlog_insert (src/backend/access/nbtree/nbtxlog.c:164)
  - btree_desc (src/backend/access/rmgrdesc/nbtdesc.c:36)
  - SizeOfBtreeInsert (src/include/access/nbtxlog.h:87)

## Notes and Other Information
- Used for four insert operation types: INSERT_LEAF (leaf page), INSERT_UPPER (internal page), INSERT_META (metadata page), and INSERT_POST (posting list split)
- INSERT_META and INSERT_UPPER operations imply non-leaf pages, while INSERT_POST and INSERT_LEAF imply leaf pages
- For INSERT_POST operations, the structure is followed by posting split offset data and the new tuple
- The WAL record includes backup blocks: original page (Blk 0), child's left sibling if needed (Blk 1), and metadata if INSERT_META (Blk 2)
- Recovery processes this record to replay the exact insertion at the specified offset