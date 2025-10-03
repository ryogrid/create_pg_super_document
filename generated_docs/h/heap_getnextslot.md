# heap_getnextslot

## Location
[src/backend/access/heap/heapam.c:1345-1374](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L1345-L1374)

## Overview
Retrieves the next tuple from a heap table scan and stores it in a provided TupleTableSlot, returning a boolean indicating whether a tuple was found.

## Definition

```c
bool
heap_getnextslot(TableScanDesc sscan, ScanDirection direction, TupleTableSlot *slot)
```
## Detailed Description
The  function is a slot-based variant of  that follows the modern PostgreSQL tuple slot interface. It performs the same core scanning logic as  but stores the result in a provided  rather than returning a  directly. The function chooses between page-mode and regular scanning based on scan flags, and handles the case where no tuple is found by clearing the slot and returning false.

This function is part of the table access method interface and provides better memory management through the slot abstraction, allowing for more efficient tuple processing in the executor.

## Parameters / Member Variables
- `sscan`: The table scan descriptor (cast to HeapScanDesc internally)
- `direction`: The scan direction (ForwardScanDirection or BackwardScanDirection)
- `*slot`: The TupleTableSlot to store the retrieved tuple
## Dependencies
- Functions called/Symbols referenced:
  - [heapgettup_pagemode](heapgettup_pagemode.md)
  - [heapgettup](heapgettup.md)
  - [ExecClearTuple](../E/ExecClearTuple.md)
  - pgstat_count_heap_getnext
  - [ExecStoreBufferHeapTuple](../E/ExecStoreBufferHeapTuple.md)
- Data structures used:
  - [HeapScanDesc](../H/HeapScanDesc.md)
  - [TableScanDesc](../T/TableScanDesc.md)
  - ScanDirection
  - [TupleTableSlot](../T/TupleTableSlot.md)
- [Scan](../S/Scan.md) flags:
  - SO_ALLOW_PAGEMODE
- Called from (representative examples):
  - [SampleHeapTupleVisible](../S/SampleHeapTupleVisible.md)
  - HeapScanIsValid

## Notes and Other Information
- Returns  if a tuple was found and stored in the slot,  if no more tuples are available
- When no tuple is found, the function clears the slot using  to ensure clean state
- Uses  to efficiently store the tuple in the slot while maintaining buffer pin
- No locking manipulations are needed as this is handled at lower levels
- Performs the same statistics tracking as  via 
- The slot-based interface allows for better memory management and integration with the executor's tuple processing pipeline
- Unlike , this function doesn't include the heap AM validation checks, suggesting it's used in more controlled contexts

## Simplified Source

```c
bool heap_getnextslot(TableScanDesc sscan, ScanDirection direction, TupleTableSlot *slot) {
    HeapScanDesc scan = (HeapScanDesc) sscan;

    // Choose scan method based on page mode flag
    if (sscan->rs_flags & SO_ALLOW_PAGEMODE)
        heapgettup_pagemode(scan, direction, sscan->rs_nkeys, sscan->rs_key);
    else
        heapgettup(scan, direction, sscan->rs_nkeys, sscan->rs_key);

    // Check if tuple was found
    if (scan->rs_ctup.t_data == NULL) {
        ExecClearTuple(slot);
        return false;
    }

    // Store tuple in slot and update statistics
    pgstat_count_heap_getnext(scan->rs_base.rs_rd);
    ExecStoreBufferHeapTuple(&scan->rs_ctup, slot, scan->rs_cbuf);
    return true;
}
```