# fsm_logical_to_physical

## Location
src/backend/storage/freespace/freespace.c: 455 - 490

## Overview
Converts a logical FSM (Free Space Map) address to its corresponding physical block number in the FSM file.

## Definition
```c
static BlockNumber fsm_logical_to_physical(FSMAddress addr)
```

## Detailed Description
This function performs the critical address translation from logical FSM coordinates (level and logical page number) to the actual physical block number where that FSM page is stored. The FSM uses a tree structure where leaf pages contain the actual free space information and upper levels contain summary information.

The algorithm works in two main phases:
1. Calculate the logical page number of the first leaf page below the given address by multiplying through the tree levels
2. Count all upper level nodes required to address up to that leaf page, accounting for the tree structure

The function handles pages at different levels of the FSM tree correctly by adjusting for the level offset and converts the final count to a 0-based block number.

## Parameters / Member Variables
- `addr`: FSMAddress structure containing the logical page number and tree level of the target FSM page

## Dependencies
- Functions called/Symbols referenced:
  - FSMAddress (structure type)
  - SlotsPerFSMPage
  - FSM_TREE_DEPTH
- Called from (representative examples):
  - FSMAddress
  - XLogRecordPageWithFreeSpace
  - FreeSpaceMapPrepareTruncateRel
  - fsm_readbuf

## Notes and Other Information
- This is a static function internal to the freespace.c module
- The FSM uses a tree structure with SlotsPerFSMPage entries per page
- The calculation accounts for all levels from leaf to root, then adjusts for the actual target level
- Returns a 0-based block number suitable for physical I/O operations
- Critical for FSM page access and maintenance operations