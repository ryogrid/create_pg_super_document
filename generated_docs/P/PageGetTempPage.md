# PageGetTempPage

## Location
[src/backend/storage/page/bufpage.c:365-381](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/page/bufpage.c#L365-L381)

## Overview
Allocates a temporary page in local memory with the same size as the source page for special processing operations.

## Definition

```c
Page
PageGetTempPage(Page page)
```
## Detailed Description
PageGetTempPage creates a temporary page buffer in local memory that matches the size of the provided source page. This function is used when algorithms need to perform operations on page data without modifying the original page in the buffer pool. The returned page is completely uninitialized, requiring the caller to set up any needed page structure or copy data from the source page. This approach provides a safe workspace for complex page operations like splits, merges, or reorganization.

## Parameters / Member Variables
- : Source page used to determine the size of the temporary page to allocate

## Dependencies
- Functions called/Symbols referenced:
  - PageGetPageSize (retrieves the size of the source page)
  - palloc (PostgreSQL memory allocation function)
- Called from (representative examples):
  - ginPlaceToPage (GIN index page placement operations)
  - dataSplitPageInternal (GIN data page splitting)
  - _bt_split (B-tree page splitting operations)

## Notes and Other Information
- Returns an uninitialized page buffer that must be set up by the caller
- The allocated memory matches the exact size of the source page
- Used primarily for temporary operations during index splits and reorganizations
- Memory is allocated in the current memory context and should be freed appropriately
- Does not copy any data from the source page - only allocates matching size
- Commonly used in conjunction with page copying or initialization functions