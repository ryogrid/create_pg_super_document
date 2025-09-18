# PageIsEmpty

## Location
src/include/storage/bufpage.h: 221 - 230

## Overview
PageIsEmpty is an inline function that determines whether a database page contains any allocated item identifiers, effectively checking if the page is empty of user data.

## Definition


## Detailed Description
This function checks if a page is empty by examining the pd_lower field of the page header. The pd_lower field indicates the offset to the start of free space on the page. If pd_lower is less than or equal to SizeOfPageHeaderData (the size of the basic page header without line pointers), it means no item identifiers have been allocated, and the page is considered empty. This is a critical function for PostgreSQL's storage management, helping determine whether pages can be reused or need special handling during operations like VACUUM.

## Parameters / Member Variables
- : A pointer to a page (Page type) to be checked for emptiness

## Dependencies
- Functions called/Symbols referenced:
  - PageHeader (casting page to PageHeaderData pointer)
  - SizeOfPageHeaderData (macro defining size of page header without line pointers)
  - PageHeaderData structure (accessed via pd_lower field)
- Called from (representative examples):
  - ginHeapTupleFastInsert (in src/backend/access/gin/ginfast.c:365)
  - gistfillbuffer (in src/backend/access/gist/gistutil.c:38)
  - lazy_scan_new_or_empty (in src/backend/access/heap/vacuumlazy.c:1323)
  - SpGistNewBuffer (in src/backend/access/spgist/spgutils.c:418)
  - PageIndexTupleDelete (in src/backend/storage/page/bufpage.c:1135)

## Notes and Other Information
- This is an inline function defined in bufpage.h for performance
- The function only checks for allocated item identifiers, not actual tuple data
- A page is considered empty if no line pointers have been allocated beyond the basic page header
- This function is widely used across different access methods (GIN, GiST, Hash, SP-GiST) and maintenance operations
- The pd_lower field tracks where free space begins, making this an efficient O(1) operation
- Used extensively in vacuum operations to identify pages that can be truncated or reused