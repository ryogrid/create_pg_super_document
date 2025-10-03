# br_page_get_freespace

## Location
[src/backend/access/brin/brin_pageops.c:916-923](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_pageops.c#L916-L923)

## Overview
Returns the amount of free space available on a regular BRIN index page, excluding pages marked for evacuation.

## Definition
```c
static Size br_page_get_freespace(Page page)
```

## Detailed Description
This function calculates the free space available on a BRIN (Block Range Index) page for storing new tuples. It serves as a safety wrapper around the generic `PageGetFreeSpace` function, adding BRIN-specific validation logic.

The function performs two key checks before returning free space:
1. Verifies the page is a regular BRIN page (not a meta page or revmap page)
2. Ensures the page is not marked with the `BRIN_EVACUATE_PAGE` flag, which indicates the page is scheduled for cleanup/evacuation

If either condition fails, the function returns 0 to prevent any new data from being inserted into the page.

## Parameters / Member Variables
- `page`: A pointer to the BRIN index page to examine for available free space

## Dependencies
- Functions called/Symbols referenced:
  - BRIN_IS_REGULAR_PAGE (macro to check if page is a regular BRIN page)
  - BrinPageFlags (function to get BRIN-specific page flags)
  - BRIN_EVACUATE_PAGE (flag constant indicating page evacuation status)
  - [PageGetFreeSpace](../P/PageGetFreeSpace.md) (generic function to calculate free space on a page)
- Called from (representative examples):
  - BrinMaxItemSize (calculates maximum item size for BRIN pages)
  - [brin_doupdate](brin_doupdate.md) (during BRIN tuple updates)
  - [brin_doinsert](brin_doinsert.md) (during BRIN tuple insertions)
  - [brin_page_cleanup](brin_page_cleanup.md) (during page cleanup operations)
  - [brin_getinsertbuffer](brin_getinsertbuffer.md) (when finding buffers for insertion)
  - [brin_initialize_empty_new_buffer](brin_initialize_empty_new_buffer.md) (during buffer initialization)

## Notes and Other Information
- This is a static function, only accessible within the brin_pageops.c file
- The function returns 0 for non-regular pages or evacuated pages to prevent data corruption
- The evacuation flag mechanism allows for safe page cleanup without blocking concurrent operations
- This function is critical for BRIN's space management and ensures data integrity during concurrent access

## Simplified Source

```c
static Size br_page_get_freespace(Page page)
{
    // Return 0 for non-regular pages or pages marked for evacuation
    if (!BRIN_IS_REGULAR_PAGE(page) ||
        (BrinPageFlags(page) & BRIN_EVACUATE_PAGE) != 0)
        return 0;
    else
        return PageGetFreeSpace(page);
}
```