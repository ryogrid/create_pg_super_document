# fsm_get_location

## Location
[src/backend/storage/freespace/freespace.c:491-505](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/freespace/freespace.c#L491-L505)

## Overview
Maps a heap block number to its corresponding FSM (Free Space Map) address and slot position within the FSM leaf page.

## Definition
```c
static FSMAddress fsm_get_location(BlockNumber heapblk, uint16 *slot)
```

## Detailed Description
This function performs the fundamental mapping from heap block numbers to FSM coordinates. Given a heap block number, it calculates which FSM leaf page contains the free space information for that block and which slot within that page corresponds to the block.

The mapping is straightforward: heap blocks are distributed across FSM leaf pages, with each FSM page containing SlotsPerFSMPage entries. The function uses integer division to determine the logical page number and modulo arithmetic to find the slot position within that page.

## Parameters / Member Variables
- `heapblk`: The heap block number to locate in the FSM
- `slot`: Output parameter that receives the slot number within the FSM page (0-based)

## Dependencies
- Functions called/Symbols referenced:
  - FSMAddress (structure type)
  - FSM_BOTTOM_LEVEL
  - SlotsPerFSMPage
- Called from (representative examples):
  - FSMAddress
  - [RecordAndGetPageWithFreeSpace](../R/RecordAndGetPageWithFreeSpace.md)
  - [RecordPageWithFreeSpace](../R/RecordPageWithFreeSpace.md)
  - [XLogRecordPageWithFreeSpace](../X/XLogRecordPageWithFreeSpace.md)
  - [GetRecordedFreeSpace](../G/GetRecordedFreeSpace.md)
  - [FreeSpaceMapPrepareTruncateRel](../F/FreeSpaceMapPrepareTruncateRel.md)
  - [fsm_vacuum_page](fsm_vacuum_page.md)

## Notes and Other Information
- This is a static function internal to the freespace.c module
- Always returns an address at FSM_BOTTOM_LEVEL (leaf level of the FSM tree)
- The slot output parameter is set to a value between 0 and SlotsPerFSMPage-1
- This mapping is the foundation for all FSM operations that need to locate heap block information
- Used extensively throughout FSM maintenance and query operations

## Simplified Source

```c
static FSMAddress fsm_get_location(BlockNumber heapblk, uint16 *slot)
{
    FSMAddress addr;

    // Always target bottom level of FSM tree
    addr.level = FSM_BOTTOM_LEVEL;

    // Map heap block to FSM coordinates
    addr.logpageno = heapblk / SlotsPerFSMPage;  // Which FSM page
    *slot = heapblk % SlotsPerFSMPage;           // Which slot in page

    return addr;
}
```