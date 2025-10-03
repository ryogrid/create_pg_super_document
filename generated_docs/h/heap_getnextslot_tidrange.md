# heap_getnextslot_tidrange

## Location
[src/backend/access/heap/heapam.c:1448-1554](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L1448-L1554)

## Overview
This function retrieves the next tuple from a heap scan within a specified TID (tuple identifier) range, filtering out tuples that fall outside the defined minimum and maximum TID bounds.

## Definition

```c
bool
heap_getnextslot_tidrange(TableScanDesc sscan, ScanDirection direction,
						  TupleTableSlot *slot)
```
## Detailed Description
heap_getnextslot_tidrange is a specialized heap scanning function that operates within a constrained TID range defined by rs_mintid and rs_maxtid in the scan descriptor. The function uses either page-mode or tuple-mode scanning depending on the SO_ALLOW_PAGEMODE flag, then filters the retrieved tuples to ensure they fall within the specified TID range.

The function performs TID range filtering by comparing each retrieved tuple's TID against the minimum and maximum bounds. When a tuple falls outside the range, it continues scanning in the appropriate direction or terminates early if no more valid tuples can exist based on the scan direction.

## Parameters / Member Variables
- `sscan`: Table scan descriptor containing scan state and TID range limits
- `direction`: Scan direction (forward or backward) determining tuple retrieval order
- `*slot`: Tuple table slot where the retrieved tuple will be stored
## Dependencies
- Functions called/Symbols referenced:
  - [heapgettup_pagemode](heapgettup_pagemode.md): Page-mode tuple retrieval
  - [heapgettup](heapgettup.md): Standard tuple-mode retrieval
  - [ItemPointerCompare](../I/ItemPointerCompare.md): TID comparison for range filtering
  - [ExecClearTuple](../E/ExecClearTuple.md): Clear tuple slot when filtering
  - ScanDirectionIsBackward/ScanDirectionIsForward: Direction checking
  - pgstat_count_heap_getnext: Statistics collection
  - [ExecStoreBufferHeapTuple](../E/ExecStoreBufferHeapTuple.md): Store tuple in slot
- Called from (representative examples):
  - [SampleHeapTupleVisible](../S/SampleHeapTupleVisible.md): Used in heap tuple sampling
  - HeapScanIsValid: Part of heap scan validation

## Notes and Other Information
- The function assumes that heap_set_tidrange has already configured the scan limits to optimize page-level filtering
- Early termination logic is direction-aware: backward scans can terminate when TIDs become too small, forward scans when TIDs become too large
- Uses either pagemode or standard tuple retrieval based on the SO_ALLOW_PAGEMODE flag
- Statistics are collected via pgstat_count_heap_getnext for performance monitoring
- Returns false when no more tuples are available within the TID range

## Simplified Source

```c
bool heap_getnextslot_tidrange(TableScanDesc sscan, ScanDirection direction,
                              TupleTableSlot *slot) {
    HeapScanDesc scan = (HeapScanDesc) sscan;
    ItemPointer mintid = &sscan->rs_mintid;
    ItemPointer maxtid = &sscan->rs_maxtid;

    for (;;) {
        // Get next tuple using appropriate scan method
        if (sscan->rs_flags & SO_ALLOW_PAGEMODE)
            heapgettup_pagemode(scan, direction, sscan->rs_nkeys, sscan->rs_key);
        else
            heapgettup(scan, direction, sscan->rs_nkeys, sscan->rs_key);

        // No more tuples
        if (scan->rs_ctup.t_data == NULL) {
            ExecClearTuple(slot);
            return false;
        }

        // Filter: TID too low
        if (ItemPointerCompare(&scan->rs_ctup.t_self, mintid) < 0) {
            ExecClearTuple(slot);
            // Backward scan: no more valid tuples possible
            if (ScanDirectionIsBackward(direction))
                return false;
            continue;
        }

        // Filter: TID too high
        if (ItemPointerCompare(&scan->rs_ctup.t_self, maxtid) > 0) {
            ExecClearTuple(slot);
            // Forward scan: no more valid tuples possible
            if (ScanDirectionIsForward(direction))
                return false;
            continue;
        }

        // Valid tuple found
        break;
    }

    // Store tuple and update statistics
    pgstat_count_heap_getnext(scan->rs_base.rs_rd);
    ExecStoreBufferHeapTuple(&scan->rs_ctup, slot, scan->rs_cbuf);
    return true;
}
```