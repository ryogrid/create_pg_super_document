# SpGistPageAddNewItem

## Location
[src/backend/access/spgist/spgutils.c:1195-1289](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgutils.c#L1195-L1289)

## Overview
Adds a new item to an SP-GiST page, with optimization to replace placeholder items when possible, helping to maintain efficient space utilization.

## Definition

```c
OffsetNumber
SpGistPageAddNewItem(SpGistState *state, Page page, Item item, Size size,
					 OffsetNumber *startOffset, bool errorOK)
```
## Detailed Description
The  function intelligently adds new items to SP-GiST index pages by first attempting to replace placeholder tuples before adding items to free space. This optimization helps reduce page fragmentation and improves space utilization. When placeholders exist and there's sufficient space (accounting for the placeholder's dead tuple size), the function searches for and replaces a placeholder. If no suitable placeholder is found or replacement fails, it falls back to adding the item to available free space.

The function includes sophisticated error handling with PANIC conditions when space calculations fail after placeholder deletion, ensuring index consistency. It also supports search optimization through a startOffset hint for repeated insertions.

## Parameters / Member Variables
- : Pointer to SpGistState containing index configuration (currently unused but maintains API consistency)
- : The target page where the item should be added
- : The item data to be inserted into the page
- : Size of the item being inserted
- : Optional hint for optimizing placeholder search; updated to next search position (can be NULL)
- : If false, function throws ERROR on failure instead of returning InvalidOffsetNumber

## Dependencies
- Functions called/Symbols referenced:
  -  - retrieves page-specific SP-GiST metadata
  -  - calculates available free space on the page
  -  - gets the highest offset number on the page
  -  - retrieves item at specific offset
  -  - gets item identifier at specific offset
  -  - removes tuple from page
  -  - adds new item to page
  -  - constant for dead tuple size
  -  - constant identifying placeholder tuples
- Called from (representative examples):
  -  - [when](../w/when.md) inserting new leaf tuples
  -  - during leaf tuple relocation operations
  -  - [when](../w/when.md) splitting nodes and distributing tuples
  -  - during node addition operations
  -  - [when](../w/when.md) performing node split actions

## Notes and Other Information
- Prioritizes placeholder replacement over free space allocation for better space utilization
- Uses MAXALIGN for proper memory alignment calculations when checking space requirements
- PANIC condition ensures index consistency when space calculations fail after placeholder deletion
- The startOffset optimization reduces repeated scanning for placeholder replacement in bulk operations
- Maintains accurate placeholder count in page opaque data for subsequent operations
- Critical for SP-GiST's space management strategy, helping to minimize page fragmentation
- Error handling varies based on errorOK parameter, supporting both strict and lenient insertion modes