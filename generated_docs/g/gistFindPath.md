# gistFindPath

## Location
src/backend/access/gist/gist.c: 909 - 1021

## Overview
Traverses the GiST tree to find the path from the root page to a specified child block, returning a stack of pages representing the path from parent to root.

## Definition


## Detailed Description
 performs a breadth-first search from the root of the GiST tree to locate a specific child block and construct the path back to the root. The function is primarily used for recovery operations when the parent-child relationship needs to be re-established.

The algorithm works by:
1. Starting from the root page and maintaining a FIFO queue of pages to visit
2. For each internal page, scanning all downlink tuples to find children 
3. If the target child is found, returning the insertion stack path
4. Otherwise, adding all child pages to the queue for further exploration
5. Handling concurrent page splits by detecting them via LSN comparison and adding newly split pages to the queue

The function implements deadlock prevention by locking only one page at a time and uses shared locks throughout the traversal.

## Parameters / Member Variables
- : The GiST index relation to search
- : The block number of the target child page to find
- : Output parameter set to the offset number of the downlink tuple in the direct parent that points to the child

## Dependencies
- Functions called/Symbols referenced:
  - gistcheckpage
  - GistFollowRight
  - GistPageGetNSN
  - GistPageGetOpaque
  - GistPageIsDeleted
  - GistPageIsLeaf
  - BufferGetLSNAtomic
  - ReadBuffer
  - LockBuffer
  - UnlockReleaseBuffer
  - PageGetMaxOffsetNumber
  - PageGetItemId
  - PageGetItem
  - ItemPointerGetBlockNumber
  - list_make1
  - list_delete_first
  - lcons
  - lappend
- Called from (representative examples):
  - gistFindCorrectParent

## Notes and Other Information
- Uses breadth-first search rather than depth-first to ensure leaf pages are encountered after all internal pages
- Includes special handling for concurrent page splits detected via LSN-NSN comparison
- The function assumes internal pages are never deleted (assertion check)
- Detects incomplete page splits and reports them as errors
- Returns NULL on failure but throws an ERROR if the child page cannot be found
- The returned insertion stack represents the path from the direct parent of the target child up to the root
- Uses a FIFO queue implemented as a PostgreSQL List to manage the breadth-first traversal