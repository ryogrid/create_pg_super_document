# PageIsNew

## Location
[src/include/storage/bufpage.h:231-240](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/bufpage.h#L231-L240)

## Overview
PageIsNew is an inline function that determines whether a database page has been initialized by checking if the page header's pd_upper field is zero.

## Definition


## Detailed Description
This function checks if a page is newly allocated and uninitialized by examining the pd_upper field in the page header. The pd_upper field indicates the offset to the end of free space on the page. When a page is first allocated from the operating system or buffer pool, it typically contains all zeros. During proper page initialization (via PageInit), pd_upper is set to a valid value indicating where free space ends. If pd_upper is still zero, it indicates the page has not been properly initialized and is considered "new". This is crucial for PostgreSQL's buffer management and crash recovery systems.

## Parameters / Member Variables
- : A pointer to a page (Page type) to be checked for initialization status

## Dependencies
- Functions called/Symbols referenced:
  - PageHeader (casting page to PageHeaderData pointer)
  - [PageHeaderData](PageHeaderData.md) structure (accessed via pd_upper field)
- Called from (representative examples):
  - [brin_start_evacuating_page](../b/brin_start_evacuating_page.md) (in src/backend/access/brin/brin_pageops.c:532)
  - [gistcheckpage](../g/gistcheckpage.md) (in src/backend/access/gist/gistutil.c:794)
  - _hash_checkpage (in src/backend/access/hash/hashutil.c:220)
  - [RelationGetBufferForTuple](../R/RelationGetBufferForTuple.md) (in src/backend/access/heap/hio.c:696)
  - [_bt_checkpage](../b/_bt_checkpage.md) (in src/backend/access/nbtree/nbtpage.c:807)
  - PageIsVerifiedExtended (in src/backend/storage/page/bufpage.c:101)

## Notes and Other Information
- This is an inline function defined in bufpage.h for performance
- Checks only the pd_upper field, which is sufficient to detect uninitialized pages
- Widely used across all PostgreSQL access methods (B-tree, Hash, GiST, SP-GiST, GIN, BRIN)
- Critical for buffer management - helps distinguish between initialized empty pages and uninitialized pages
- Used in crash recovery to determine if pages need special handling
- Essential for checksum validation - new/uninitialized pages may not have valid checksums
- The function assumes that zero is an invalid value for pd_upper in initialized pages
- Used in vacuum operations to identify pages that need initialization before use