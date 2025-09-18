# brin_start_evacuating_page

## Location
src/backend/access/brin/brin_pageops.c: 524 - 563

## Overview
Initiates the page evacuation protocol for a BRIN index page by marking it as unsuitable for new tuple insertions when it contains existing tuples.

## Definition
```c
bool brin_start_evacuating_page(Relation idxRel, Buffer buf)
```

## Detailed Description
This function implements the first phase of BRIN page evacuation by examining a page's contents and conditionally marking it for evacuation. The evacuation protocol is used when a regular BRIN index page needs to be repurposed for the reverse mapping (revmap). 

The function performs the following operations:
1. Checks if the page is new/uninitialized - if so, returns false as no evacuation is needed
2. Iterates through all item pointers on the page to detect any used tuples
3. If any used tuples are found, sets the BRIN_EVACUATE_PAGE flag to prevent new tuple insertions
4. Marks the buffer as dirty with a hint to indicate the change

The BRIN_EVACUATE_PAGE flag serves as a marker that informs other functions (particularly br_page_get_freespace) that this page can no longer be used for new tuple insertions, effectively starting the evacuation process.

## Parameters / Member Variables
- `idxRel`: Relation structure representing the BRIN index (currently unused in the function body)
- `buf`: Buffer containing the page to potentially evacuate, must be exclusively locked by caller

## Dependencies
- Functions called/Symbols referenced:
  - BufferGetPage (to access the page from buffer)
  - PageIsNew (to check if page is uninitialized)
  - PageGetMaxOffsetNumber (to get the highest item offset)
  - PageGetItemId (to access item pointers)
  - ItemIdIsUsed (to check if an item pointer is in use)
  - BrinPageFlags (to access page flags)
  - MarkBufferDirtyHint (to mark buffer as modified)
  - BRIN_EVACUATE_PAGE (flag constant)
  - FirstOffsetNumber (starting offset constant)
- Called from:
  - revmap_physical_extend (in brin_revmap.c)

## Notes and Other Information
- The caller must hold an exclusive lock on the buffer before calling this function
- Returns true if evacuation was initiated (page had tuples), false if page was empty/new
- The BRIN_EVACUATE_PAGE flag is not explicitly WAL-logged, but may be logged accidentally through other operations
- This function only marks the page for evacuation; actual tuple evacuation is handled by other functions
- Part of the BRIN index page management system that allows repurposing regular index pages for reverse mapping when needed