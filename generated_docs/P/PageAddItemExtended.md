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
- `page`: Pointer to the page where the item will be added
- `item`: Pointer to the item data to be inserted
- `size`: Size of the item in bytes
- `offsetNumber`: Target offset for placement (InvalidOffsetNumber for automatic assignment)
- `flags`: Control flags (PAI_OVERWRITE, PAI_IS_HEAP) that modify insertion behavior
## Dependencies
- Functions called/Symbols referenced:
  - PageHeader (page header type)
  - [PageGetMaxOffsetNumber](PageGetMaxOffsetNumber.md) (gets highest used offset)
  - OffsetNumberNext (calculates next offset number)
  - OffsetNumberIsValid (validates offset numbers)
  - [PageGetItemId](PageGetItemId.md) (retrieves line pointer)
  - ItemIdIsUsed, ItemIdHasStorage (item status checks)
  - [PageHasFreeLinePointers](PageHasFreeLinePointers.md) (checks for available line pointers)
  - [PageClearHasFreeLinePointers](PageClearHasFreeLinePointers.md) (clears free pointer hint)
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

## Simplified Source

```c
OffsetNumber PageAddItemExtended(Page page, Item item, Size size,
                                OffsetNumber offsetNumber, int flags)
{
    PageHeader phdr = (PageHeader) page;
    Size alignedSize;
    int lower, upper;
    ItemId itemId;
    OffsetNumber limit;
    bool needshuffle = false;

    // Validate page structure integrity
    if (phdr->pd_lower < SizeOfPageHeaderData ||
        phdr->pd_lower > phdr->pd_upper ||
        phdr->pd_upper > phdr->pd_special ||
        phdr->pd_special > BLCKSZ) {
        ereport(PANIC, (errmsg("corrupted page pointers")));
    }

    // Determine where to place the item
    limit = OffsetNumberNext(PageGetMaxOffsetNumber(page));

    if (OffsetNumberIsValid(offsetNumber)) {
        // Use specified offset
        if ((flags & PAI_OVERWRITE) != 0) {
            // Check if we can overwrite at this position
            if (offsetNumber < limit) {
                itemId = PageGetItemId(page, offsetNumber);
                if (ItemIdIsUsed(itemId) || ItemIdHasStorage(itemId)) {
                    return InvalidOffsetNumber;  // Can't overwrite used slot
                }
            }
        } else {
            // Insert at position, may need to shuffle existing items
            if (offsetNumber < limit)
                needshuffle = true;
        }
    } else {
        // Find a free slot automatically
        offsetNumber = limit;  // Default to end
        if (PageHasFreeLinePointers(page)) {
            // Search for recyclable unused slot
            for (offsetNumber = FirstOffsetNumber; offsetNumber < limit; offsetNumber++) {
                itemId = PageGetItemId(page, offsetNumber);
                if (!ItemIdIsUsed(itemId) && !ItemIdHasStorage(itemId))
                    break;
            }
            if (offsetNumber >= limit) {
                PageClearHasFreeLinePointers(page);  // Reset hint
                offsetNumber = limit;
            }
        }
    }

    // Validate placement constraints
    if (offsetNumber > limit ||
        ((flags & PAI_IS_HEAP) != 0 && offsetNumber > MaxHeapTuplesPerPage)) {
        return InvalidOffsetNumber;
    }

    // Calculate space requirements
    if (offsetNumber == limit || needshuffle)
        lower = phdr->pd_lower + sizeof(ItemIdData);
    else
        lower = phdr->pd_lower;

    alignedSize = MAXALIGN(size);
    upper = (int) phdr->pd_upper - (int) alignedSize;

    if (lower > upper)
        return InvalidOffsetNumber;  // No space

    // Insert the item
    itemId = PageGetItemId(page, offsetNumber);

    if (needshuffle) {
        // Move existing line pointers to make room
        memmove(itemId + 1, itemId, (limit - offsetNumber) * sizeof(ItemIdData));
    }

    // Set up line pointer and copy data
    ItemIdSetNormal(itemId, upper, size);
    memcpy((char *) page + upper, item, size);

    // Update page boundaries
    phdr->pd_lower = (LocationIndex) lower;
    phdr->pd_upper = (LocationIndex) upper;

    return offsetNumber;
}
```