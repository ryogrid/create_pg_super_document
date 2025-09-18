# _bt_split

## Location
[src/backend/access/nbtree/nbtinsert.c:1467-2098](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtinsert.c#L1467-L2098)

## Overview
_bt_split splits a B-tree page when insufficient space exists for a new tuple, creating a new right sibling page and redistributing tuples between the original (left) and new (right) pages.

## Definition


## Detailed Description
This function performs the complex operation of splitting a B-tree page into two pages. The process involves several critical steps:

1. **Split Point Selection**: Uses _bt_findsplitloc() to determine the optimal point to split the page, balancing the distribution of tuples between left and right pages.

2. **Page Setup**: Creates a temporary left page and allocates a new right page buffer. The original page becomes the left page, and tuples are redistributed accordingly.

3. **High Key Management**: 
   - For leaf pages: Creates a truncated high key using suffix truncation when possible
   - For internal pages: Uses the first right tuple directly as the high key to maintain separator key integrity

4. **Tuple Distribution**: Iterates through all tuples and distributes them to appropriate pages based on the split point, handling special cases like posting list splits.

5. **Sibling Link Updates**: Updates prev/next pointers to maintain the doubly-linked list structure of pages at the same level.

6. **WAL Logging**: Records all changes in a comprehensive WAL record for crash recovery, including specialized handling for posting list splits.

The function ensures atomicity through critical sections and handles complex scenarios like concurrent posting list splits.

## Parameters / Member Variables
- : The B-tree index relation being split
- : The heap relation referenced by the index
- : BTScanInsert structure used for suffix truncation on leaf pages (NULL for internal pages)
- : Buffer containing the page to be split (pinned and write-locked)
- : Left-sibling buffer when splitting non-leaf page (used to clear INCOMPLETE_SPLIT flag)
- : Offset where the new item should be inserted
- : Size of the new item being inserted
- : The new IndexTuple to be inserted
- : Original new item when posting list split is involved
- : New posting list tuple when posting list split is involved  
- : Offset within posting list for posting list splits (0 if not applicable)

## Dependencies
- Functions called/Symbols referenced:
  - [_bt_findsplitloc](_bt_findsplitloc.md) (to determine split point)
  - [_bt_allocbuf](_bt_allocbuf.md) (to allocate new right page)
  - [_bt_truncate](_bt_truncate.md) (for suffix truncation on leaf pages)
  - [_bt_pgaddtup](_bt_pgaddtup.md) (to add tuples to pages)
  - PageGetTempPage, PageRestoreTempPage (for temporary page management)
  - [XLogBeginInsert](../X/XLogBeginInsert.md), XLogRegisterBuffer, XLogInsert (for WAL logging)
  - Various page and buffer management functions
- Called from (representative examples):
  - [_bt_insertonpg](_bt_insertonpg.md) (when page split is needed during insertion)

## Notes and Other Information
- Returns the new right sibling buffer, pinned and write-locked
- The original buffer (left page) remains pinned and write-locked
- Uses critical sections to ensure atomicity of the split operation
- Handles both leaf and internal page splits with different logic for high key creation
- Supports concurrent posting list splits through special parameter handling
- Updates sibling page links and handles INCOMPLETE_SPLIT flag clearing
- Includes extensive error handling with proper cleanup of allocated resources
- The function is static and only used within the nbtinsert.c module
- Maintains B-tree invariants including page ordering and key distribution