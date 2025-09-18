# ginDeletePage

## Location
src/backend/access/gin/ginvacuum.c: 130 - 246

## Overview
Deletes a posting tree page from a GIN index by unlinking it from the tree structure and marking it as deleted for eventual reuse.

## Definition


## Detailed Description
This static function performs the complete deletion of a posting tree page in a GIN index. It handles the complex process of safely removing a page from the B-tree structure while maintaining consistency and crash recovery. The function requires that the parent page holds an exclusive cleanup lock to guarantee no concurrent insertions occur in the subtree during deletion.

The deletion process involves multiple steps: unlinking the page from its siblings by updating the left sibling's rightlink, removing the downlink from the parent page, marking the page as deleted with a transaction ID, and creating appropriate WAL records for crash recovery. The function also handles predicate locking to ensure that any inserts that would have gone to the deleted page are redirected to its right sibling.

## Parameters / Member Variables
- : GinVacuumState containing index context, buffer strategy, and result statistics
- : Block number of the page to be deleted
- : Block number of the left sibling page that needs rightlink update
- : Block number of the parent page containing the downlink to remove
- : Offset in the parent page of the downlink pointing to the page being deleted
- : Boolean indicating whether the parent page is the root (currently unused in function body)

## Dependencies
- Functions called/Symbols referenced:
  - ReadBufferExtended (read pages into buffers)
  - BufferGetPage (get page from buffer)
  - GinPageGetOpaque (access page opaque data)
  - PredicateLockPageCombine (handle predicate locking)
  - GinDataPageGetPostingItem (get posting item from page)
  - PostingItemGetBlockNumber (extract block number from posting item)
  - GinPageDeletePostingItem (remove posting item from page)
  - GinPageSetDeleted (mark page as deleted)
  - GinPageSetDeleteXid (set deletion transaction ID)
  - ReadNextTransactionId (get next transaction ID)
  - MarkBufferDirty (mark buffers as modified)
  - XLogBeginInsert/XLogRegisterBuffer/XLogInsert (WAL logging)
  - ReleaseBuffer (release buffer references)
- Called from (representative examples):
  - ginScanToDelete

## Notes and Other Information
- Static function, only accessible within ginvacuum.c
- Requires exclusive cleanup lock on parent page before calling
- Updates vacuum statistics (pages_newly_deleted, pages_deleted)
- Handles special WAL registration due to pd_lower issues in pre-9.4 binary-upgraded pages
- Preserves rightlink in deleted page to maintain workability of running search scans
- Uses critical section to ensure atomicity of the deletion operation
- Includes debug assertions to verify the correct posting item is being deleted