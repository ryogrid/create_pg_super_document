# gistendscan

## Location
[src/backend/access/gist/gistscan.c:349-358](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistscan.c#L349-L358)

## Overview
Terminates a GiST index scan and cleans up all associated resources, including memory contexts and state structures.

## Definition

```c
void
gistendscan(IndexScanDesc scan)
```
## Detailed Description
This function serves as the cleanup routine for GiST index scans, implementing the final phase of the index access method API. It performs comprehensive resource deallocation by calling freeGISTstate(), which recursively cleans up all memory contexts, data structures, and temporary resources that were allocated during the scan's lifetime. The function follows PostgreSQL's memory management patterns where hierarchical memory contexts ensure that a single cleanup operation can safely deallocate all related resources without memory leaks.

The simplicity of this function belies its importance in the scan lifecycle - it ensures that all memory allocated in gistbeginscan() and gistrescan() is properly released, including the search queue context, scan-lifetime allocations, and any index-only scan infrastructure.

## Parameters / Member Variables
- : The IndexScanDesc structure representing the scan to be terminated

## Dependencies
- Functions called/Symbols referenced:
  - [freeGISTstate](../f/freeGISTstate.md)
  - GISTScanOpaque (type)
  - [IndexScanDesc](../I/IndexScanDesc.md) (type)
- Called from (representative examples):
  - [gisthandler](gisthandler.md)

## Notes and Other Information
- Relies on PostgreSQL's hierarchical memory context system for complete cleanup
- Automatically handles both regular scans and index-only scans
- The freeGISTstate() call handles cleanup of all subsidiary contexts (scanCxt, queueCxt, tempCxt, pageDataCxt)
- Part of the standard index access method API, ensuring consistent resource management across all index types
- No explicit return value needed as cleanup operations are designed to be infallible within the PostgreSQL memory management framework