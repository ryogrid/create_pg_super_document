# PageAddItemExtended

## Location
[src/backend/storage/page/bufpage.c:194-364](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/page/bufpage.c#L194-L364)

## Overview
Adds an item to a PostgreSQL page at a specified or automatically determined offset, handling space allocation, line pointer management, and data placement with comprehensive validation.

## Definition

```c
OffsetNumber
PageAddItemExtended(Page page,
					Item item,
					Size size,
					OffsetNumber offsetNumber,
					int flags)
```
## Detailed Description
PageAddItemExtended is the core function for adding items to PostgreSQL pages. It manages the complex task of finding appropriate storage locations, updating line pointer arrays, and ensuring proper page structure integrity. The function supports both automatic offset assignment (by finding free line pointers) and explicit placement at specified offsets. It handles line pointer recycling, space validation, and can optionally move existing items to make room for new ones. The function includes extensive corruption detection and enforces heap-specific constraints when requested.

## Parameters / Member Variables
- : Pointer to the page where the item will be added
- : Pointer to the item data to be inserted
- : Size of the item in bytes
- : Target offset for placement (InvalidOffsetNumber for automatic assignment)
- : Control flags (PAI_OVERWRITE, PAI_IS_HEAP) that modify insertion behavior

## Dependencies
- Functions called/Symbols referenced:
  - PageHeader (page header type)
  - PageGetMaxOffsetNumber (gets highest used offset)
  - OffsetNumberNext (calculates next offset number)
  - OffsetNumberIsValid (validates offset numbers)
  - PageGetItemId (retrieves line pointer)
  - ItemIdIsUsed, ItemIdHasStorage (item status checks)
  - PageHasFreeLinePointers (checks for available line pointers)
  - PageClearHasFreeLinePointers (clears free pointer hint)
  - ItemIdSetNormal (sets line pointer values)
  - MAXALIGN (alignment macro)
  - VALGRIND_CHECK_MEM_IS_DEFINED (memory validation)
- Called from (representative examples):
  - PageAddItem (convenience wrapper function)

## Notes and Other Information
- Returns InvalidOffsetNumber on failure, valid OffsetNumber on success
- Enforces strict page corruption detection with PANIC on invalid pointers
- Supports line pointer recycling to reuse previously deleted slots efficiently
- PAI_OVERWRITE flag allows replacement of existing unused line pointers
- PAI_IS_HEAP flag enforces MaxHeapTuplesPerPage constraint for heap tables
- Automatically handles line pointer array expansion when adding beyond current limit
- Uses memmove for safe overlapping memory operations when shuffling line pointers
- Includes Valgrind integration for detecting uninitialized memory access
- Updates both pd_lower and pd_upper boundaries to reflect new page state
- WARNING: ereport(ERROR) is explicitly disallowed in this function to prevent corruption during critical operations