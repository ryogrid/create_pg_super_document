# fsm_get_heap_blk

## Location
[src/backend/storage/freespace/freespace.c:506-516](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/freespace/freespace.c#L506-L516)

## Overview
Converts an FSM (Free Space Map) address and slot position back to the corresponding heap block number, performing the inverse operation of fsm_get_location.

## Definition
```c
static BlockNumber fsm_get_heap_blk(FSMAddress addr, uint16 slot)
```

## Detailed Description
This function performs the reverse mapping from FSM coordinates to heap block numbers. Given an FSM address (which must be at the bottom level) and a slot within that FSM page, it calculates which heap block that slot represents.

The calculation is straightforward: multiply the logical page number by the number of slots per FSM page and add the slot offset. This gives the exact heap block number that corresponds to that FSM slot position.

## Parameters / Member Variables
- `addr`: FSMAddress structure containing the logical page number and level (must be FSM_BOTTOM_LEVEL)
- `slot`: The slot number within the FSM page (0-based, must be < SlotsPerFSMPage)

## Dependencies
- Functions called/Symbols referenced:
  - FSMAddress (structure type)
  - FSM_BOTTOM_LEVEL
  - SlotsPerFSMPage
- Called from (representative examples):
  - FSMAddress
  - [RecordAndGetPageWithFreeSpace](../R/RecordAndGetPageWithFreeSpace.md)
  - [fsm_search](fsm_search.md)

## Notes and Other Information
- This is a static function internal to the freespace.c module
- Contains an Assert to ensure the address is at the bottom level (leaf level) of the FSM tree
- The cast to unsigned int prevents potential overflow issues with the multiplication
- This function is the exact inverse of fsm_get_location()
- Used primarily during FSM searches to convert found slots back to heap block numbers
- Critical for translating FSM search results back to actionable heap block addresses

## Simplified Source

```c
static BlockNumber fsm_get_heap_blk(FSMAddress addr, uint16 slot)
{
    // Must be at bottom level of FSM tree
    Assert(addr.level == FSM_BOTTOM_LEVEL);

    // Convert FSM coordinates to heap block number
    return ((unsigned int) addr.logpageno) * SlotsPerFSMPage + slot;
}
```