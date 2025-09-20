# tbm_free

## Location
[src/backend/nodes/tidbitmap.c:322-340](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/tidbitmap.c#L322-L340)

## Overview
Deallocates all memory associated with a TIDBitmap structure and frees the bitmap itself.

## Definition

```c
void
tbm_free(TIDBitmap *tbm)
```
## Detailed Description
The `tbm_free` function performs complete cleanup of a TIDBitmap structure, deallocating all associated memory resources. This includes the main hash table (pagetable), any shared page arrays (spages), shared chunk arrays (schunks), and finally the TIDBitmap structure itself. The function ensures proper cleanup regardless of which internal representation the bitmap was using (single page, hash table, or shared memory structures).

This is a critical memory management function that prevents memory leaks when bitmap heap scans are completed or aborted.

## Parameters / Member Variables
- `tbm`: Pointer to the TIDBitmap structure to be freed

## Dependencies
- Functions called/Symbols referenced:
  - pagetable_destroy
  - [pfree](../p/pfree.md)
  - [TIDBitmap](../T/TIDBitmap.md) (struct type)
- Called from (representative examples):
  - [startScanEntry](../s/startScanEntry.md)
  - [ginFreeScanKeys](../g/ginFreeScanKeys.md)
  - [MultiExecBitmapAnd](../M/MultiExecBitmapAnd.md)
  - [ExecReScanBitmapHeapScan](../E/ExecReScanBitmapHeapScan.md)
  - [ExecEndBitmapHeapScan](../E/ExecEndBitmapHeapScan.md)
  - [MultiExecBitmapOr](../M/MultiExecBitmapOr.md)

## Notes and Other Information
- Handles cleanup of all possible internal representations of the bitmap
- Safely checks for NULL pointers before freeing (pagetable, spages, schunks)
- Must be called when bitmap operations are complete to prevent memory leaks
- Used in both normal execution completion and error cleanup paths
- The function does not handle DSA (Dynamic Shared Area) cleanup - that's handled separately by tbm_free_shared_area
- Called from various executor nodes including bitmap AND, OR, and heap scan operations