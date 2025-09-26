# PageGetTempPageCopy

## Location
src/backend/storage/page/bufpage.c: 382 - 401

## Overview
Allocates a temporary page in local memory and initializes it by copying the complete contents from the source page.

## Definition

```c
Page
PageGetTempPageCopy(Page page)
```
## Detailed Description
PageGetTempPageCopy creates a temporary page buffer in local memory that is an exact copy of the provided source page. Unlike PageGetTempPage which returns uninitialized memory, this function performs a complete bitwise copy of the source page including all headers, line pointers, and item data. This is particularly useful for algorithms that need to work with a complete page copy while preserving the original page structure, such as during page reorganization or split operations where the original page layout needs to be preserved.

## Parameters / Member Variables
- : Source page to be copied to the temporary page buffer

## Dependencies
- Functions called/Symbols referenced:
  - PageGetPageSize (retrieves the size of the source page)
  - palloc (PostgreSQL memory allocation function)
  - memcpy (standard memory copy function)
- Called from (representative examples):
  - entrySplitPage (GIN entry page splitting operations)
  - ginVacuumEntryPage (GIN entry page vacuum operations)

## Notes and Other Information
- Returns a complete copy of the source page in newly allocated memory
- The copied page contains identical header information, line pointers, and item data
- Memory is allocated in the current memory context and should be freed when no longer needed
- Commonly used in index maintenance operations where the original page structure must be preserved
- More expensive than PageGetTempPage due to the complete memory copy operation
- The resulting page can be modified independently without affecting the source page