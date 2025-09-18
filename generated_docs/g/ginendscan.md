# ginendscan

## Location
src/backend/access/gin/ginscan.c: 506 - 516

## Overview
Terminates a GIN index scan and performs complete cleanup of all associated memory contexts and data structures.

## Definition
```c
void ginendscan(IndexScanDesc scan)
```

## Detailed Description
The `ginendscan` function is responsible for the complete cleanup and termination of a GIN index scan operation. This function is called when an index scan is finished and all associated resources need to be freed to prevent memory leaks.

The function performs a systematic cleanup process:

1. **Scan Key Cleanup**: Frees all scan key data structures and their associated memory using `ginFreeScanKeys`
2. **Memory Context Cleanup**: Deletes the temporary and key memory contexts that were created during scan initialization
3. **Opaque Structure Cleanup**: Frees the main GIN scan opaque structure that holds all scan-specific state

This cleanup is essential for proper memory management in long-running PostgreSQL sessions where many index scans may be performed. The function ensures that all memory allocated during the scan lifecycle is properly returned to the system.

## Parameters / Member Variables
- `scan`: IndexScanDesc structure representing the index scan to be terminated

## Dependencies
- Functions called/Symbols referenced:
  - `[ginFreeScanKeys](ginFreeScanKeys.md)`: Frees scan key structures and associated memory
  - `[MemoryContextDelete](../M/MemoryContextDelete.md)`: Deletes memory contexts (called twice for tempCtx and keyCtx)
  - `[pfree](../p/pfree.md)`: Frees the main opaque scan structure
- Called from (representative examples):
  - `[ginhandler](ginhandler.md)`: Part of the index access method interface

## Notes and Other Information
- This function is part of the standard index access method interface and is automatically called by the PostgreSQL executor when a scan completes
- The function assumes that the scan has been properly initialized and that all referenced memory contexts exist
- After this function returns, the IndexScanDesc structure itself may be reused for other scans, but all GIN-specific state will have been cleared
- Proper cleanup is crucial for preventing memory leaks in systems that perform many index scans
- The order of cleanup operations is important: scan keys are freed first, then memory contexts, and finally the main structure