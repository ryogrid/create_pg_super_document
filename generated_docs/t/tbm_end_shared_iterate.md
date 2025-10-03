# tbm_end_shared_iterate

## Location
[src/backend/nodes/tidbitmap.c:1158-1168](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/tidbitmap.c#L1158-L1168)

## Overview
Finishes a shared iteration over a TIDBitmap by cleaning up the backend-private iterator state while leaving shared state intact.

## Definition

```c
void
tbm_end_shared_iterate(TBMSharedIterator *iterator)
```
## Detailed Description
The  function serves as the cleanup routine for shared TIDBitmap iterations. Unlike , this function only deallocates the local backend-private state while preserving the shared memory state that may still be accessed by other processes.

This distinction is crucial in parallel processing scenarios where multiple processes share the same bitmap iteration state. Each process maintains its own TBMSharedIterator structure pointing to shared memory segments, but only cleans up its local references when finished.

## Parameters / Member Variables
- `*iterator`: TBMSharedIterator pointer containing backend-private state to be deallocated
## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md)
  - [TBMSharedIterator](../T/TBMSharedIterator.md)
  - [PagetableEntry](../P/PagetableEntry.md)
- Called from (representative examples):
  - [BitmapPrefetch](../B/BitmapPrefetch.md) (src/backend/executor/nodeBitmapHeapscan.c:538)
  - [ExecReScanBitmapHeapScan](../E/ExecReScanBitmapHeapScan.md) (src/backend/executor/nodeBitmapHeapscan.c:609, 611)
  - [ExecEndBitmapHeapScan](../E/ExecEndBitmapHeapScan.md) (src/backend/executor/nodeBitmapHeapscan.c:664, 666)

## Notes and Other Information
Critical for parallel bitmap scans where shared memory state must persist beyond individual process cleanup. The shared state (including the actual bitmap data and shared iterator state) remains valid for other processes. Always pair with appropriate shared iterator initialization. The function does not acquire locks since it only affects local process state.

## Simplified Source

```c
void tbm_end_shared_iterate(TBMSharedIterator *iterator) {
    // Clean up only backend-private state, leave shared state intact
    pfree(iterator);
}
```