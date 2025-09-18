# PageGetSpecialSize

## Location
src/include/storage/bufpage.h: 314 - 324

## Overview
Calculates and returns the size of the special space area on a page by determining the difference between total page size and the special space offset.

## Definition
static inline uint16 PageGetSpecialSize(Page page)

## Detailed Description
PageGetSpecialSize computes the size of the special space area at the end of a page by subtracting the pd_special offset from the total page size. The special space is used by various PostgreSQL access methods (btree, hash, GiST, etc.) to store method-specific metadata and control information. The calculation is: total_page_size - pd_special_offset = special_space_size.

## Parameters / Member Variables
- page: A Page pointer to the formatted page from which to calculate the special space size

## Dependencies
- Functions called/Symbols referenced:
  - PageGetPageSize (to get the total page size)
  - PageHeader (type cast to access pd_special field)
- Called from (representative examples):
  - gistcheckpage (GiST index page validation)
  - _hash_checkpage (Hash index page validation)
  - _bt_checkpage (B-tree index page validation)
  - PageGetTempPageCopySpecial (temporary page operations)

## Notes and Other Information
- This is an inline function defined in bufpage.h for performance
- The special space is located at the end of the page, growing backwards from the page end
- Different access methods use different amounts of special space for their metadata
- Used primarily in index access methods for page validation and special space management
- The pd_special field in PageHeader contains the offset where special space begins
- A pd_special value equal to page size indicates no special space is allocated
- Part of the page special data function family in PostgreSQL storage system