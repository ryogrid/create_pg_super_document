# IndexScanEnd

## Location
src/backend/access/index/genam.c: 144 - 175

## Overview
Releases the storage and resources allocated by RelationGetIndexScan, completing the cleanup of an index scan operation.

## Definition


## Detailed Description
IndexScanEnd is the complementary function to RelationGetIndexScan that handles the deallocation of memory and cleanup of an IndexScanDesc structure. This function is called as part of the index scan termination process, but importantly, it assumes that any access method (AM) specific resources have already been released by the AM's own endscan routine. The function performs a clean shutdown by freeing the scan key workspace and order-by data that were allocated during scan initialization, then freeing the scan descriptor itself.

This function is deliberately simple and focused solely on memory management, as the complex AM-specific cleanup is handled elsewhere in the scanning infrastructure.

## Parameters / Member Variables
- : The IndexScanDesc structure to be deallocated and cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - pfree (memory deallocation)
  - IndexScanDesc (scan descriptor type)
- Called from (representative examples):
  - index_endscan (high-level index scan termination)
  - IndexScanIsValid (validation macro)

## Notes and Other Information
- This function must be called after the AM's endscan routine has already cleaned up AM-specific resources
- The function safely handles NULL pointers for keyData and orderByData fields
- This is part of the two-phase scan termination process: AM endscan first, then IndexScanEnd
- The function does not perform any locking operations - those are handled by the AM's endscan routine
- Memory is freed using pfree, PostgreSQL's standard memory deallocation function