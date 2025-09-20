# _bt_insert_parent

## Location
[src/backend/access/nbtree/nbtinsert.c:2099-2240](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtinsert.c#L2099-L2240)

## Overview
_bt_insert_parent completes a page split by inserting a downlink to the new right page into the appropriate parent page, handling both normal splits and root splits.

## Definition

```c
struction of a new root.  If our stack is empty
	 * then we have just split a node on what had been the root level when we
	 * descended the tree.  If it was still the root then we perform a
	 * new-root construction.  If it *wasn't* the root anymore, search to find
	 * the next higher level that someone constructed meanwhile, and find the
	 * right place to insert as for the normal case.
	 *
	 * If we have to search for the parent level, we do so by re-descending
	 * from the root.  This is not super-efficient, but it's rare enough not
	 * to matter.
	 */
	if (isroot)
	{
		Buffer		rootbuf;

		Assert(stack == NULL);
		Assert(isonly);
		/* create a new root node one level up and update the metapage */
		rootbuf = _bt_newlevel(rel, heaprel, buf, rbuf);
		/* release the split buffers */
		_bt_relbuf(rel, rootbuf);
		_bt_relbuf(rel, rbuf);
		_bt_relbuf(rel, buf);
	}
	else
	{
		BlockNumber bknum = BufferGetBlockNumber(buf);
		BlockNumber rbknum = BufferGetBlockNumber(rbuf);
		Page		page = BufferGetPage(buf);
		IndexTuple	new_item;
		BTStackData fakestack;
		IndexTuple	ritem;
		Buffer		pbuf;

		if (stack == NULL)
		{
			BTPageOpaque opaque;

			elog(DEBUG2, "concurrent ROOT page split");
			opaque = BTPageGetOpaque(page);

			/*
			 * We should never reach here when a leaf page split takes place
			 * despite the insert of newitem being able to apply the fastpath
			 * optimization.  Make sure of that with an assertion.
			 *
			 * This is more of a performance issue than a correctness issue.
			 * The fastpath won't have a descent stack.  Using a phony stack
			 * here works, but never rely on that.  The fastpath should be
			 * rejected within _bt_search_insert() when the rightmost leaf
			 * page will split, since it's faster to go through _bt_search()
			 * and get a stack in the usual way.
			 */
			Assert(!(P_ISLEAF(opaque) &&
					 BlockNumberIsValid(RelationGetTargetBlock(rel))));

			/* Find the leftmost page at the next level up */
			pbuf = _bt_get_endpoint(rel, opaque->btpo_level + 1, false);
			/* Set up a phony stack entry pointing there */
			stack = &fakestack;
			stack->bts_blkno = BufferGetBlockNumber(pbuf);
			stack->bts_offset = InvalidOffsetNumber;
			stack->bts_parent = NULL;
			_bt_relbuf(rel, pbuf);
		}

		/* get high key from left, a strict lower bound for new right page */
		ritem = (IndexTuple) PageGetItem(page,
										 PageGetItemId(page, P_HIKEY));

		/* form an index tuple that points at the new right page */
		new_item = CopyIndexTuple(ritem);
		BTreeTupleSetDownLink(new_item, rbknum);

		/*
		 * Re-find and write lock the parent of buf.
		 *
		 * It's possible that the location of buf's downlink has changed since
		 * our initial _bt_search() descent.  _bt_getstackbuf() will detect
		 * and recover from this, updating the stack, which ensures that the
		 * new downlink will be inserted at the correct offset. Even buf's
		 * parent may have changed.
		 */
		pbuf = _bt_getstackbuf(rel, heaprel, stack, bknum);

		/*
		 * Unlock the right child.  The left child will be unlocked in
		 * _bt_insertonpg().
		 *
		 * Unlocking the right child must be delayed until here to ensure that
		 * no concurrent VACUUM operation can become confused.  Page deletion
		 * cannot be allowed to fail to re-find a downlink for the rbuf page.
		 * (Actually, this is just a vestige of how things used to work.  The
		 * page deletion code is expected to check for the INCOMPLETE_SPLIT
		 * flag on the left child.  It won't attempt deletion of the right
		 * child until the split is complete.  Despite all this, we opt to
		 * conservatively delay unlocking the right child until here.)
		 */
		_bt_relbuf(rel, rbuf);

		if (pbuf == InvalidBuffer)
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg_internal("failed to re-find parent key in index \"%s\" for split pages %u/%u",
									 RelationGetRelationName(rel), bknum, rbknum)));

		/* Recursively insert into the parent */
		_bt_insertonpg(rel, heaprel, NULL, pbuf, buf, stack->bts_parent,
					   new_item, MAXALIGN(IndexTupleSize(new_item)),
					   stack->bts_offset + 1, 0, isonly);

		/* be tidy */
		pfree(new_item);
	}
}

/*
 * _bt_finish_split() -- Finish an incomplete split
 *
 * A crash or other failure can leave a split incomplete.  The insertion
 * routines won't allow to insert on a page that is incompletely split.
 * Before inserting on such a page, call _bt_finish_split().
 *
 * On entry, 'lbuf' must be locked in write-mode.  On exit, it is unlocked
 * and unpinned.
 *
 * Caller must provide a valid heaprel, since finishing a page split requires
 * allocating a new page if and when the parent page splits in turn.
 */
void
_bt_finish_split(Relation rel, Relation heaprel, Buffer lbuf, BTStack stack)
{
	Page		lpage = BufferGetPage(lbuf);
```
## Detailed Description
This function is responsible for the final step of page splitting: inserting the appropriate downlink into the parent page to make the split permanent and visible to other operations. The function handles two main scenarios:

1. **Root Split Handling**: When isroot is true, it means we've split the root page itself. In this case, a completely new root level must be created using _bt_newlevel(), which creates a new root page containing downlinks to both the old and new pages.

2. **Normal Parent Insertion**: For non-root splits, the function:
   - Re-finds the parent page using _bt_getstackbuf() (since the parent location may have changed during concurrent operations)
   - Creates a new index tuple containing the high key from the left page and a downlink to the new right page
   - Recursively calls _bt_insertonpg() to insert this downlink into the parent

The function handles edge cases like concurrent root splits where the stack might be NULL, requiring reconstruction of parent information. It carefully manages buffer locks to prevent concurrent VACUUM operations from becoming confused during the split process.

## Parameters / Member Variables
- : The B-tree index relation being modified
- : The heap relation referenced by the index
- : Buffer containing the left (original) page from the split
- : Buffer containing the new right page from the split
- : BTStack containing parent page information (NULL for root splits or concurrent operations)
- : True if we split the actual root page
- : True if we split a page that was alone on its level (might have been fast root)

## Dependencies
- Functions called/Symbols referenced:
  - [_bt_newlevel](_bt_newlevel.md) (for creating new root level)
  - [_bt_getstackbuf](_bt_getstackbuf.md) (to re-find and lock parent page)
  - [_bt_insertonpg](_bt_insertonpg.md) (recursive call to insert downlink)
  - [_bt_get_endpoint](_bt_get_endpoint.md) (to find leftmost page when stack is NULL)
  - [CopyIndexTuple](../C/CopyIndexTuple.md), BTreeTupleSetDownLink (to create parent downlink tuple)
  - Buffer management functions (_bt_relbuf)
- Called from (representative examples):
  - [_bt_insertonpg](_bt_insertonpg.md) (after page split completion)
  - [_bt_finish_split](_bt_finish_split.md) (during split completion in certain scenarios)

## Notes and Other Information
- This is a static function within nbtinsert.c, not exposed externally
- Releases both buf and rbuf buffer locks upon completion
- Handles concurrent operations gracefully by re-finding parent pages when necessary
- The function ensures atomicity by delaying the release of the right child buffer until parent insertion is ready
- For root splits, it creates an entirely new tree level and updates the metapage accordingly
- When stack is NULL due to concurrent operations, it constructs a "fake" stack to enable normal processing
- The function maintains B-tree invariants by ensuring proper downlink insertion and INCOMPLETE_SPLIT flag management
- Includes assertions to catch performance issues with fastpath optimization usage
- Error handling includes corruption detection when parent re-finding fails