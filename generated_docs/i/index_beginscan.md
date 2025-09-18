# index_beginscan

## Location
src/backend/access/index/indexam.c: 256 - 286

## Overview
The  function initiates a new index scan operation using the amgettuple access method, setting up the necessary structures for tuple-by-tuple index traversal.

## Definition


## Detailed Description
This function creates and initializes an index scan descriptor for scanning an index with tuple-level access. It serves as a high-level interface that internally calls  and then sets up additional parameters specific to heap relation access. The function prepares the scan for fetching index matches from the underlying table by initializing the heap fetch mechanism. The caller must hold appropriate locks on both the heap relation and index relation before calling this function.

## Parameters / Member Variables
- : The heap relation associated with the index being scanned
- : The index relation to be scanned
- : The snapshot to use for visibility checking during the scan (must not be InvalidSnapshot)
- : Number of scan keys (search conditions) for the scan
- : Number of ordering specifications for the scan

## Dependencies
- Functions called/Symbols referenced:
  - [index_beginscan_internal](index_beginscan_internal.md) (internal scan initialization)
  - table_index_fetch_begin (heap fetch preparation)
  - InvalidSnapshot (constant for validation)
  - [IndexScanDesc](../I/IndexScanDesc.md) (return type structure)
- Called from (representative examples):
  - [systable_beginscan](../s/systable_beginscan.md) (src/backend/access/index/genam.c:442)
  - [systable_beginscan_ordered](../s/systable_beginscan_ordered.md) (src/backend/access/index/genam.c:700)
  - [IndexNext](../I/IndexNext.md) (src/backend/executor/nodeIndexscan.c:109)
  - [IndexOnlyNext](../I/IndexOnlyNext.md) (src/backend/executor/nodeIndexonlyscan.c:92)

## Notes and Other Information
- Part of PostgreSQL's index access method abstraction layer
- Requires valid snapshot parameter - assertion will fail if InvalidSnapshot is passed
- The returned IndexScanDesc must be properly closed with index_endscan when scan is complete
- Sets up both index scanning and heap tuple fetching mechanisms
- Located in src/backend/access/index/indexam.c:256-286