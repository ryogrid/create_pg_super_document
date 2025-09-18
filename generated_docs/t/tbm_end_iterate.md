# tbm_end_iterate

## Location
src/backend/nodes/tidbitmap.c: 1146 - 1157

## Overview
Finishes an iteration over a TIDBitmap by cleaning up the private iterator resources.

## Definition


## Detailed Description
The  function serves as the cleanup routine for TIDBitmap iterations. Currently, it simply deallocates the iterator memory using , but the design allows for future enhancements such as tracking open iterators or enabling the bitmap to return to read/write status when no active iterators remain.

This function is essential for proper memory management in bitmap scan operations and should always be called when iteration is complete to prevent memory leaks.

## Parameters / Member Variables
- : TBMIterator pointer to be deallocated and cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - pfree
  - TBMIterator
- Called from (representative examples):
  - startScanEntry (src/backend/access/gin/ginget.c:376)
  - entryGetItem (src/backend/access/gin/ginget.c:842)
  - ginFreeScanKeys (src/backend/access/gin/ginscan.c:254)
  - BitmapPrefetch (src/backend/executor/nodeBitmapHeapscan.c:481)
  - ExecReScanBitmapHeapScan (src/backend/executor/nodeBitmapHeapscan.c:605, 607)
  - ExecEndBitmapHeapScan (src/backend/executor/nodeBitmapHeapscan.c:658, 660)

## Notes and Other Information
This is a simple but important cleanup function that could be extended in future versions to provide more sophisticated iterator management. Always pair with  to ensure proper resource management. For shared iterators, use  instead.