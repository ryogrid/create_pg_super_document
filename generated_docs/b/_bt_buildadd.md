# _bt_buildadd

## Location
[src/backend/access/nbtree/nbtsort.c:784-1028](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsort.c#L784-L1028)

## Overview
A core function that adds an item to a disk page during B-tree index construction, handling page splits, high key management, and proper page layout according to B-tree conventions.

## Definition

```c
static void
_bt_buildadd(BTWriteState *wstate, BTPageState *state, IndexTuple itup,
			 Size truncextra)
```
## Detailed Description
This function is responsible for adding items to pages during B-tree index building from sorted output. It implements the complex logic required to maintain proper B-tree page layout conventions while efficiently building the index structure.

The function handles several critical aspects of B-tree construction:

1. **Page Layout Management**: Ensures proper layout conventions where rightmost pages start data items at P_HIKEY instead of P_FIRSTKEY, and on non-leaf pages, the key portion of the first item need not be stored.

2. **Page Splitting Logic**: When a page becomes full (either due to hard size limits or soft fillfactor limits), it creates a new page, properly distributes items, and establishes parent-child relationships.

3. **High Key Management**: For leaf pages, implements suffix truncation to create optimized high keys by calling , which can significantly reduce storage requirements.

4. **Tree Level Management**: Automatically creates new B-tree levels when needed by establishing parent pages and maintaining the tree structure.

5. **Sibling Link Management**: Properly sets up the doubly-linked list structure between sibling pages at each level.

The function contains detailed logic for handling posting lists and considers their impact on page space calculations, particularly important for the soft fillfactor limit.

## Parameters / Member Variables
- `*wstate`: BTWriteState structure containing the overall state of the index building operation
- `*state`: BTPageState structure containing the state for the current page being built
- `itup`: The IndexTuple to be added to the current page
- `truncextra`: Size of any posting list in the tuple, used for space calculations and truncation decisions
## Dependencies
- Functions called/Symbols referenced:
  - CHECK_FOR_INTERRUPTS
  - [PageGetFreeSpace](../P/PageGetFreeSpace.md)
  - IndexTupleSize
  - MAXALIGN
  - BTMaxItemSize
  - [_bt_check_third_page](_bt_check_third_page.md)
  - [_bt_blnewpage](_bt_blnewpage.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - [PageGetItem](../P/PageGetItem.md)
  - [_bt_sortaddtup](_bt_sortaddtup.md)
  - ItemIdGetLength
  - ItemIdSetUnused
  - [_bt_truncate](_bt_truncate.md)
  - [PageIndexTupleOverwrite](../P/PageIndexTupleOverwrite.md)
  - [_bt_pagestate](_bt_pagestate.md)
  - BTreeTupleGetNAtts
  - [BTreeTupleSetDownLink](../B/BTreeTupleSetDownLink.md)
  - [CopyIndexTuple](../C/CopyIndexTuple.md)
  - BTPageGetOpaque
  - [_bt_blwritepage](_bt_blwritepage.md)
  - OffsetNumberNext
  - [palloc0](../p/palloc0.md)
  - [BTreeTupleSetNAtts](../B/BTreeTupleSetNAtts.md)
- Called from (representative examples):
  - [_bt_buildadd](_bt_buildadd.md) (recursive call for parent pages)
  - [_bt_sort_dedup_finish_pending](_bt_sort_dedup_finish_pending.md)
  - [_bt_uppershutdown](_bt_uppershutdown.md)
  - [_bt_load](_bt_load.md)

## Notes and Other Information
- This function implements a recursive algorithm where page splits can trigger additional calls to handle parent page updates
- The function carefully manages memory allocation and deallocation, particularly for truncated high keys
- Space calculations consider both hard limits (maximum tuple size) and soft limits (fillfactor)
- Leaf pages receive special treatment for suffix truncation to optimize storage efficiency
- The function ensures that pages maintain the minimum required number of items (at least 2 non-pivot tuples plus a high key)
- Page splits preserve the B-tree invariant that all items on a page fall within the range defined by the low and high keys
- The truncextra parameter optimization helps make better decisions about when to finish pages based on potential space savings from truncating posting lists

## Simplified Source

```c
static void
_bt_buildadd(BTWriteState *wstate, BTPageState *state, IndexTuple itup,
             Size truncextra)
{
    BulkWriteBuffer nbuf;
    Page npage;
    BlockNumber nblkno;
    OffsetNumber last_off;
    Size pgspc;
    Size itupsz;
    bool isleaf;

    CHECK_FOR_INTERRUPTS();

    // Get current page information
    nbuf = state->btps_buf;
    npage = (Page) nbuf;
    nblkno = state->btps_blkno;
    last_off = state->btps_lastoff;
    state->btps_lastextra = truncextra;

    // Calculate space requirements
    pgspc = PageGetFreeSpace(npage);
    itupsz = MAXALIGN(IndexTupleSize(itup));
    isleaf = (state->btps_level == 0);

    // Check if tuple fits on any page at all
    if (unlikely(itupsz > BTMaxItemSize(npage)))
        _bt_check_third_page(wstate->index, wstate->heap, isleaf, npage, itup);

    // Check if current page needs to be split
    if (pgspc < itupsz + (isleaf ? MAXALIGN(sizeof(ItemPointerData)) : 0) ||
        (pgspc + state->btps_lastextra < state->btps_full && last_off > P_FIRSTKEY)) {

        // Split the page - finish current page and create new one
        BulkWriteBuffer obuf = nbuf;
        Page opage = npage;
        BlockNumber oblkno = nblkno;
        ItemId ii, hii;
        IndexTuple oitup;

        // Create new page
        nbuf = _bt_blnewpage(wstate, state->btps_level);
        npage = (Page) nbuf;
        nblkno = wstate->btws_pages_alloced++;

        // Move last item to new page and make it high key of old page
        Assert(last_off > P_FIRSTKEY);
        ii = PageGetItemId(opage, last_off);
        oitup = (IndexTuple) PageGetItem(opage, ii);
        _bt_sortaddtup(npage, ItemIdGetLength(ii), oitup, P_FIRSTKEY, !isleaf);

        // Set up high key on old page
        hii = PageGetItemId(opage, P_HIKEY);
        *hii = *ii;
        ItemIdSetUnused(ii);
        ((PageHeader) opage)->pd_lower -= sizeof(ItemIdData);

        // For leaf pages, truncate the high key to save space
        if (isleaf) {
            IndexTuple lastleft, truncated;
            ii = PageGetItemId(opage, OffsetNumberPrev(last_off));
            lastleft = (IndexTuple) PageGetItem(opage, ii);

            truncated = _bt_truncate(wstate->index, lastleft, oitup, wstate->inskey);
            if (!PageIndexTupleOverwrite(opage, P_HIKEY, (Item) truncated,
                                         IndexTupleSize(truncated)))
                elog(ERROR, "failed to add high key to the index page");
            pfree(truncated);

            hii = PageGetItemId(opage, P_HIKEY);
            oitup = (IndexTuple) PageGetItem(opage, hii);
        }

        // Create parent level if needed
        if (state->btps_next == NULL)
            state->btps_next = _bt_pagestate(wstate, state->btps_level + 1);

        // Link old page into parent
        BTreeTupleSetDownLink(state->btps_lowkey, oblkno);
        _bt_buildadd(wstate, state->btps_next, state->btps_lowkey, 0);
        pfree(state->btps_lowkey);

        // Save high key as new page's low key
        state->btps_lowkey = CopyIndexTuple(oitup);

        // Set sibling links
        BTPageOpaque oopaque = BTPageGetOpaque(opage);
        BTPageOpaque nopaque = BTPageGetOpaque(npage);
        oopaque->btpo_next = nblkno;
        nopaque->btpo_prev = oblkno;
        nopaque->btpo_next = P_NONE;

        // Write out the old page
        _bt_blwritepage(wstate, obuf, oblkno);

        last_off = P_FIRSTKEY;
    }

    // Set up low key for first item on page
    if (last_off == P_HIKEY) {
        Assert(state->btps_lowkey == NULL);
        state->btps_lowkey = palloc0(sizeof(IndexTupleData));
        state->btps_lowkey->t_info = sizeof(IndexTupleData);
        BTreeTupleSetNAtts(state->btps_lowkey, 0, false);
    }

    // Add the new item to current page
    last_off = OffsetNumberNext(last_off);
    _bt_sortaddtup(npage, itupsz, itup, last_off,
                   !isleaf && last_off == P_FIRSTKEY);

    // Update state
    state->btps_buf = nbuf;
    state->btps_blkno = nblkno;
    state->btps_lastoff = last_off;
}
```