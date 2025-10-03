# gistfixsplit

## Location
[src/backend/access/gist/gist.c:1195-1254](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gist.c#L1195-L1254)

## Overview
Completes an incomplete page split that was left unfinished by a previous backend crash, ensuring GiST tree consistency by inserting the missing downlinks to parent pages.

## Definition

```c
struct a
	 * downlink tuple for each page.
	 */
	for (;
```
## Detailed Description
 handles the recovery of incomplete page splits in GiST indexes. When a backend crashes during a page split operation, it may leave split pages connected by right-links but without proper downlinks in the parent page. This function:

1. **Detection**: Recognizes incomplete splits by checking the  flag on pages
2. **Chain Traversal**: Follows the right-link chain to find all pages that were part of the incomplete split
3. **Downlink Creation**: For each page in the split chain, creates appropriate downlink tuples using 
4. **Split Completion**: Calls  to insert all the downlinks into the parent page(s)

The function ensures that the tree remains consistent and accessible even after system crashes during split operations. It logs the recovery operation for diagnostic purposes.

## Parameters / Member Variables
- : Current GiST insertion state containing the stack and other context information
- : GiST-specific state information including operator classes and support functions

## Dependencies
- Functions called/Symbols referenced:
  - [gistformdownlink](gistformdownlink.md)
  - [gistfinishsplit](gistfinishsplit.md)
  - GistFollowRight
  - GistPageGetOpaque
  - OffsetNumberIsValid
  - [BufferGetPage](../B/BufferGetPage.md)
  - [ReadBuffer](../R/ReadBuffer.md)
  - [LockBuffer](../L/LockBuffer.md)
  - RelationGetRelationName
  - ereport
  - [lappend](../l/lappend.md)
  - [palloc](../p/palloc.md)
- Called from (representative examples):
  - [gistdoinsert](gistdoinsert.md)

## Notes and Other Information
- This function is crucial for crash recovery and maintaining GiST tree consistency
- The operation is logged at LOG level for monitoring incomplete split recovery
- Uses assertions to verify that the page actually needs split completion
- Maintains proper locking while traversing the split chain to prevent concurrent issues
- The function only handles splits that were interrupted before downlink insertion
- Works by collecting information about all split pages before attempting to fix the parent
- Critical for ensuring that split pages become properly accessible through normal tree traversal after recovery

## Simplified Source
```c
static void gistfixsplit(GISTInsertState *state, GISTSTATE *giststate) {
    GISTInsertStack *stack = state->stack;
    Buffer buf;
    Page page;
    List *splitinfo = NIL;

    // Log the recovery operation
    ereport(LOG,
            (errmsg("fixing incomplete split in index \"%s\", block %u",
                    RelationGetRelationName(state->r), stack->blkno)));

    // Validate this is actually an incomplete split
    Assert(GistFollowRight(stack->page));
    Assert(OffsetNumberIsValid(stack->downlinkoffnum));

    buf = stack->buffer;

    // Traverse the chain of split pages and create downlinks
    for (;;) {
        GISTPageSplitInfo *si = palloc(sizeof(GISTPageSplitInfo));
        IndexTuple downlink;

        page = BufferGetPage(buf);

        // Create downlink tuple for this page
        downlink = gistformdownlink(state->r, buf, giststate, stack,
                                   state->is_build);

        // Add to split info list
        si->buf = buf;
        si->downlink = downlink;
        splitinfo = lappend(splitinfo, si);

        // Follow right link to next split page if it exists
        if (GistFollowRight(page)) {
            buf = ReadBuffer(state->r, GistPageGetOpaque(page)->rightlink);
            LockBuffer(buf, GIST_EXCLUSIVE);
        } else {
            break;  // End of split chain
        }
    }

    // Insert all the downlinks to complete the split
    gistfinishsplit(state, stack, giststate, splitinfo, false);
}
```