# _bt_walk_left

## Location
[src/backend/access/nbtree/nbtsearch.c:2378-2491](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsearch.c#L2378-L2491)

## Overview
Steps left one page in a B-tree index structure, handling various edge cases such as deleted pages and concurrent modifications during page traversal.

## Definition

```c
static Buffer
_bt_walk_left(Relation rel, Buffer buf)
```
## Detailed Description
This function performs a leftward traversal in a B-tree index, moving from the current page to its left sibling. It implements sophisticated logic to handle concurrent operations that may occur during traversal, including page deletions and splits. The function uses a robust recovery mechanism when the expected left sibling is not found, implementing a limited rightward search to locate the correct page.

The algorithm handles the complexity of concurrent B-tree modifications by checking page validity and implementing retry logic. When a page is found to be deleted, it continues searching rightward to find the first non-deleted page that has acquired the deleted page's keyspace. The function includes safeguards against infinite loops and provides clear error messages when structural inconsistencies are detected.

## Parameters / Member Variables
- `rel`: Relation - The B-tree index relation being traversed
- `buf`: Buffer - The current page buffer (must be pinned and read-locked on entry)
## Dependencies
- Functions called/Symbols referenced:
  - BTPageGetOpaque
  - P_LEFTMOST
  - P_ISDELETED  
  - P_RIGHTMOST
  - [_bt_relbuf](_bt_relbuf.md)
  - [_bt_getbuf](_bt_getbuf.md)
  - [_bt_relandgetbuf](_bt_relandgetbuf.md)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
  - CHECK_FOR_INTERRUPTS
  - BTPageOpaque (type)
  - BT_READ (constant)
- Called from (representative examples):
  - [_bt_readnextpage](_bt_readnextpage.md)

## Notes and Other Information
- Returns InvalidBuffer if no left page exists or if traversal fails
- The input buffer is always released before attempting to step left
- On successful return, the caller has pin and read lock on the returned page
- Implements a "four hops" limit when searching for the correct sibling page to prevent excessive traversal
- The returned leaf page may be half-dead; callers must check this condition
- Handles concurrent page deletions and splits gracefully through retry mechanisms
- Uses CHECK_FOR_INTERRUPTS() to allow query cancellation during potentially long operations
- This is a static function only accessible within nbtsearch.c

## Simplified Source

```c
static Buffer
_bt_walk_left(Relation rel, Buffer buf)
{
    Page page;
    BTPageOpaque opaque;

    page = BufferGetPage(buf);
    opaque = BTPageGetOpaque(page);

    for (;;) {
        BlockNumber originalBlock;
        BlockNumber leftBlock;
        BlockNumber currentBlock;
        int retryCount;

        // Check if we're at the leftmost page
        if (P_LEFTMOST(opaque)) {
            _bt_relbuf(rel, buf);
            break;
        }

        // Remember current page and step left
        originalBlock = BufferGetBlockNumber(buf);
        currentBlock = leftBlock = opaque->btpo_prev;
        _bt_relbuf(rel, buf);

        CHECK_FOR_INTERRUPTS();

        // Get the left page
        buf = _bt_getbuf(rel, currentBlock, BT_READ);
        page = BufferGetPage(buf);
        opaque = BTPageGetOpaque(page);

        // Search for correct sibling page (limited to 4 hops)
        retryCount = 0;
        for (;;) {
            if (!P_ISDELETED(opaque) && opaque->btpo_next == originalBlock) {
                return buf;  // Found the correct page
            }

            if (P_RIGHTMOST(opaque) || ++retryCount > 4)
                break;

            currentBlock = opaque->btpo_next;
            buf = _bt_relandgetbuf(rel, buf, currentBlock, BT_READ);
            page = BufferGetPage(buf);
            opaque = BTPageGetOpaque(page);
        }

        // Return to original page to check its status
        buf = _bt_relandgetbuf(rel, buf, originalBlock, BT_READ);
        page = BufferGetPage(buf);
        opaque = BTPageGetOpaque(page);

        if (P_ISDELETED(opaque)) {
            // Original page was deleted, find first non-deleted page
            for (;;) {
                if (P_RIGHTMOST(opaque))
                    elog(ERROR, "fell off the end of index \"%s\"",
                         RelationGetRelationName(rel));

                currentBlock = opaque->btpo_next;
                buf = _bt_relandgetbuf(rel, buf, currentBlock, BT_READ);
                page = BufferGetPage(buf);
                opaque = BTPageGetOpaque(page);

                if (!P_ISDELETED(opaque))
                    break;
            }
            // Continue loop with new starting point
        } else {
            // Check for infinite loop condition
            if (opaque->btpo_prev == leftBlock)
                elog(ERROR, "could not find left sibling of block %u in index \"%s\"",
                     originalBlock, RelationGetRelationName(rel));
            // Retry with updated left block pointer
        }
    }

    return InvalidBuffer;
}
```