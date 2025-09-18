# heap_tuple_needs_eventual_freeze

## Location
[src/backend/access/heap/heapam.c:7787-7841](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L7787-L7841)

## Overview
heap_tuple_needs_eventual_freeze determines whether a tuple contains transaction IDs that will eventually require freezing to prevent wraparound issues.

## Definition


## Detailed Description
This function examines all transaction ID fields in a tuple header (xmin, xmax, xvac) to determine if any contain normal transaction IDs that will eventually need to be frozen. Transaction ID freezing is a critical PostgreSQL maintenance operation that prevents transaction ID wraparound by converting old transaction IDs to special frozen values.

The function checks:
1. **xmin field**: If it contains a normal transaction ID, the tuple needs freezing
2. **xmax field**: If it contains a normal transaction ID or a valid multixact ID, the tuple needs freezing  
3. **xvac field**: If the tuple was moved (HEAP_MOVED flag) and xvac contains a normal transaction ID, the tuple needs freezing

A tuple that passes all these checks is considered to not need eventual freezing, meaning it either already contains frozen values or special non-normal transaction IDs.

## Parameters / Member Variables
- : Pointer to the tuple header to examine for freezing requirements

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderGetXmin
  - HeapTupleHeaderGetRawXmax
  - HeapTupleHeaderGetXvac
  - TransactionIdIsNormal
  - MultiXactIdIsValid
  - HEAP_XMAX_IS_MULTI
  - HEAP_MOVED
- Called from (representative examples):
  - [heap_page_is_all_visible](heap_page_is_all_visible.md)
  - HeapScanIsValid

## Notes and Other Information
This function is essential for PostgreSQL's vacuum and freeze operations. It helps determine which tuples can be skipped during freezing operations and which pages can be marked as all-visible. The function only checks if freezing will *eventually* be needed - it doesn't determine if freezing is immediately required. This is used in visibility map maintenance and lazy vacuum optimization to identify pages that contain unfrozen data.