# gistdoinsert

## Location
[src/backend/access/gist/gist.c:634-908](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gist.c#L634-L908)

## Overview
This is the workhouse routine for inserting a tuple into a GiST (Generalized Search Tree) index, handling the complex tree traversal, page splits, and concurrency control required for safe index insertion.

## Definition

```c
void
gistdoinsert(Relation r, IndexTuple itup, Size freespace,
			 GISTSTATE *giststate, Relation heapRel, bool is_build)
```
## Detailed Description
 performs the core GiST index insertion logic by walking down the tree from the root, following the path of smallest penalty to find the appropriate leaf page for insertion. The function handles several complex scenarios:

1. **Tree Traversal**: Starts from the root and descends the tree using  to select the best child node at each internal page based on insertion penalty.

2. **Concurrency Control**: Uses a sophisticated locking protocol with shared/exclusive lock upgrades and LSN-based consistency checking to handle concurrent operations safely.

3. **Split Recovery**: Detects and fixes incomplete page splits left by crashed backends using .

4. **Parent Updates**: Updates parent node keys along the descent path when necessary to maintain tree consistency.

5. **Page Split Handling**: Manages page splits during insertion and handles the complex retry logic when splits occur.

The function operates in a short-lived memory context and doesn't bother releasing palloc'd allocations, assuming cleanup will happen when the context is destroyed.

## Parameters / Member Variables
- : The GiST index relation being inserted into
- : The index tuple to be inserted
- : Amount of free space required on the target page
- : GiST-specific state information including operator classes and support functions
- : The heap relation corresponding to this index
- : Boolean indicating whether this insertion is part of an index build operation

## Dependencies
- Functions called/Symbols referenced:
  - [gistcheckpage](gistcheckpage.md)
  - [gistchoose](gistchoose.md)  
  - [gistfixsplit](gistfixsplit.md)
  - [gistgetadjusted](gistgetadjusted.md)
  - [gistinserttuple](gistinserttuple.md)
  - GistFollowRight
  - GistPageGetNSN
  - GistPageIsDeleted
  - GistPageIsLeaf
  - GistTupleIsInvalid
  - [BufferGetLSNAtomic](../B/BufferGetLSNAtomic.md)
  - [PageGetLSN](../P/PageGetLSN.md)
  - [ReadBuffer](../R/ReadBuffer.md)
  - [LockBuffer](../L/LockBuffer.md)
  - [ReleaseBuffer](../R/ReleaseBuffer.md)
- Called from (representative examples):
  - [gistinsert](gistinsert.md)
  - [gistBuildCallback](gistBuildCallback.md)

## Notes and Other Information
- The function implements an optimistic locking strategy, acquiring shared locks initially and upgrading to exclusive locks only when modifications are needed
- LSN-NSN interlocks are used to detect concurrent page splits and trigger appropriate retry logic
- The function handles the special case of root page splits differently from internal page splits
- During index builds, LSN checking is bypassed since LSNs are not updated
- The retry mechanism ensures consistency even in the presence of concurrent operations and system crashes
- Invalid tuples from pre-PostgreSQL 9.1 installations are detected and reported as errors requiring a REINDEX

## Simplified Source
```c
void gistdoinsert(Relation r, IndexTuple itup, Size freespace,
                  GISTSTATE *giststate, Relation heapRel, bool is_build) {
    GISTInsertStack firststack;
    GISTInsertStack *stack;
    GISTInsertState state;
    bool xlocked = false;

    // Initialize state for tree traversal
    memset(&state, 0, sizeof(GISTInsertState));
    state.freespace = freespace;
    state.r = r;
    state.heapRel = heapRel;
    state.is_build = is_build;

    // Start traversal from root
    firststack.blkno = GIST_ROOT_BLKNO;
    firststack.lsn = 0;
    firststack.retry_from_parent = false;
    firststack.parent = NULL;
    state.stack = stack = &firststack;

    // Main tree traversal loop
    for (;;) {
        // Handle retry requests from failed operations
        while (stack->retry_from_parent) {
            if (xlocked)
                LockBuffer(stack->buffer, GIST_UNLOCK);
            xlocked = false;
            ReleaseBuffer(stack->buffer);
            state.stack = stack = stack->parent;
        }

        // Read page if not already cached
        if (XLogRecPtrIsInvalid(stack->lsn))
            stack->buffer = ReadBuffer(state.r, stack->blkno);

        // Lock page (optimistically start with shared lock)
        if (!xlocked) {
            LockBuffer(stack->buffer, GIST_SHARE);
            gistcheckpage(state.r, stack->buffer);
        }

        stack->page = BufferGetPage(stack->buffer);
        stack->lsn = xlocked ? PageGetLSN(stack->page) :
                              BufferGetLSNAtomic(stack->buffer);

        // Fix incomplete splits from crashed backends
        if (GistFollowRight(stack->page)) {
            if (!xlocked) {
                LockBuffer(stack->buffer, GIST_UNLOCK);
                LockBuffer(stack->buffer, GIST_EXCLUSIVE);
                xlocked = true;
                if (!GistFollowRight(stack->page))
                    continue;
            }
            gistfixsplit(&state, giststate);
            UnlockReleaseBuffer(stack->buffer);
            xlocked = false;
            state.stack = stack = stack->parent;
            continue;
        }

        // Check for concurrent changes via LSN comparison
        if ((stack->blkno != GIST_ROOT_BLKNO &&
             stack->parent->lsn < GistPageGetNSN(stack->page)) ||
            GistPageIsDeleted(stack->page)) {
            // Page changed concurrently, retry from parent
            UnlockReleaseBuffer(stack->buffer);
            xlocked = false;
            state.stack = stack = stack->parent;
            continue;
        }

        if (!GistPageIsLeaf(stack->page)) {
            // Internal page - find best child and continue descent
            BlockNumber childblkno;
            IndexTuple newtup;
            GISTInsertStack *item;
            OffsetNumber downlinkoffnum;

            // Choose best child based on insertion penalty
            downlinkoffnum = gistchoose(state.r, stack->page, itup, giststate);
            ItemId iid = PageGetItemId(stack->page, downlinkoffnum);
            IndexTuple idxtuple = (IndexTuple) PageGetItem(stack->page, iid);
            childblkno = ItemPointerGetBlockNumber(&(idxtuple->t_tid));

            // Validate tuple (detect pre-9.1 invalid tuples)
            if (GistTupleIsInvalid(idxtuple))
                ereport(ERROR, (errmsg("invalid inner tuple found, REINDEX required")));

            // Update parent key if necessary
            newtup = gistgetadjusted(state.r, idxtuple, itup, giststate);
            if (newtup) {
                // Upgrade to exclusive lock for modification
                if (!xlocked) {
                    LockBuffer(stack->buffer, GIST_UNLOCK);
                    LockBuffer(stack->buffer, GIST_EXCLUSIVE);
                    xlocked = true;
                    stack->page = BufferGetPage(stack->buffer);

                    // Retry if page changed during lock upgrade
                    if (PageGetLSN(stack->page) != stack->lsn)
                        continue;
                }

                // Update parent tuple (may cause page split)
                if (gistinserttuple(&state, stack, giststate, newtup,
                                   downlinkoffnum)) {
                    // Split occurred, retry navigation
                    if (stack->blkno != GIST_ROOT_BLKNO) {
                        UnlockReleaseBuffer(stack->buffer);
                        xlocked = false;
                        state.stack = stack = stack->parent;
                    }
                    continue;
                }
            }

            LockBuffer(stack->buffer, GIST_UNLOCK);
            xlocked = false;

            // Descend to chosen child
            item = (GISTInsertStack *) palloc0(sizeof(GISTInsertStack));
            item->blkno = childblkno;
            item->parent = stack;
            item->downlinkoffnum = downlinkoffnum;
            state.stack = stack = item;

        } else {
            // Leaf page - perform final insertion

            // Upgrade to exclusive lock for modification
            if (!xlocked) {
                LockBuffer(stack->buffer, GIST_UNLOCK);
                LockBuffer(stack->buffer, GIST_EXCLUSIVE);
                xlocked = true;
                stack->page = BufferGetPage(stack->buffer);
                stack->lsn = PageGetLSN(stack->page);

                // Handle special case: root page might have become internal
                if (stack->blkno == GIST_ROOT_BLKNO &&
                    !GistPageIsLeaf(stack->page)) {
                    LockBuffer(stack->buffer, GIST_UNLOCK);
                    xlocked = false;
                    continue;
                }

                // Check for concurrent changes on non-root pages
                if (stack->blkno != GIST_ROOT_BLKNO &&
                    (GistFollowRight(stack->page) ||
                     stack->parent->lsn < GistPageGetNSN(stack->page) ||
                     GistPageIsDeleted(stack->page))) {
                    UnlockReleaseBuffer(stack->buffer);
                    xlocked = false;
                    state.stack = stack = stack->parent;
                    continue;
                }
            }

            // Insert tuple into leaf page
            gistinserttuple(&state, stack, giststate, itup,
                           InvalidOffsetNumber);
            LockBuffer(stack->buffer, GIST_UNLOCK);

            // Release all pins and exit
            for (; stack; stack = stack->parent)
                ReleaseBuffer(stack->buffer);
            break;
        }
    }
}
```