# _bt_pagedel

## Location
[src/backend/access/nbtree/nbtpage.c:1802-2087](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtpage.c#L1802-L2087)

## Overview
This function performs the complete deletion of a leaf page from a B-tree index, coordinating both the marking phase and unlinking phase of page deletion while maintaining index integrity.

## Definition

```c
struct a search stack to scanblkno.  On subsequent iterations,
		 * we know we stepped right from a page that passed these tests, so
		 * it's OK.
		 */
		if (P_RIGHTMOST(opaque) || P_ISROOT(opaque) ||
			P_FIRSTDATAKEY(opaque) <= PageGetMaxOffsetNumber(page) ||
			P_INCOMPLETE_SPLIT(opaque))
		{
			/* Should never fail to delete a half-dead page */
			Assert(!P_ISHALFDEAD(opaque));

			_bt_relbuf(rel, leafbuf);
			return;
		}

		/*
		 * First, remove downlink pointing to the page (or a parent of the
		 * page, if we are going to delete a taller subtree), and mark the
		 * leafbuf page half-dead
		 */
		if (!P_ISHALFDEAD(opaque))
		{
			/*
			 * We need an approximate pointer to the page's parent page.  We
			 * use a variant of the standard search mechanism to search for
			 * the page's high key; this will give us a link to either the
			 * current parent or someplace to its left (if there are multiple
			 * equal high keys, which is possible with !heapkeyspace indexes).
			 *
			 * Also check if this is the right-half of an incomplete split
			 * (see comment above).
			 */
			if (!stack)
			{
				BTScanInsert itup_key;
				ItemId		itemid;
				IndexTuple	targetkey;
				BlockNumber leftsib,
							leafblkno;
				Buffer		sleafbuf;

				itemid = PageGetItemId(page, P_HIKEY);
				targetkey = CopyIndexTuple((IndexTuple) PageGetItem(page, itemid));

				leftsib = opaque->btpo_prev;
				leafblkno = BufferGetBlockNumber(leafbuf);

				/*
				 * To avoid deadlocks, we'd better drop the leaf page lock
				 * before going further.
				 */
				_bt_unlockbuf(rel, leafbuf);

				/*
				 * Check that the left sibling of leafbuf (if any) is not
				 * marked with INCOMPLETE_SPLIT flag before proceeding
				 */
				Assert(leafblkno == scanblkno);
				if (_bt_leftsib_splitflag(rel, leftsib, leafblkno))
				{
					ReleaseBuffer(leafbuf);
					return;
				}

				/*
				 * We need an insertion scan key, so build one.
				 *
				 * _bt_search searches for the leaf page that contains any
				 * matching non-pivot tuples, but we need it to "search" for
				 * the high key pivot from the page that we're set to delete.
				 * Compensate for the mismatch by having _bt_search locate the
				 * last position < equal-to-untruncated-prefix non-pivots.
				 */
				itup_key = _bt_mkscankey(rel, targetkey);

				/* Set up a BTLessStrategyNumber-like insertion scan key */
				itup_key->nextkey = false;
				itup_key->backward = true;
				stack = _bt_search(rel, NULL, itup_key, &sleafbuf, BT_READ);
				/* won't need a second lock or pin on leafbuf */
				_bt_relbuf(rel, sleafbuf);

				/*
				 * Re-lock the leaf page, and start over to use our stack
				 * within _bt_mark_page_halfdead.  We must do it that way
				 * because it's possible that leafbuf can no longer be
				 * deleted.  We need to recheck.
				 *
				 * Note: We can't simply hold on to the sleafbuf lock instead,
				 * because it's barely possible that sleafbuf is not the same
				 * page as leafbuf.  This happens when leafbuf split after our
				 * original lock was dropped, but before _bt_search finished
				 * its descent.  We rely on the assumption that we'll find
				 * leafbuf isn't safe to delete anymore in this scenario.
				 * (Page deletion can cope with the stack being to the left of
				 * leafbuf, but not to the right of leafbuf.)
				 */
				_bt_lockbuf(rel, leafbuf, BT_WRITE);
				continue;
			}

			/*
			 * See if it's safe to delete the leaf page, and determine how
			 * many parent/internal pages above the leaf level will be
			 * deleted.  If it's safe then _bt_mark_page_halfdead will also
			 * perform the first phase of deletion, which includes marking the
			 * leafbuf page half-dead.
			 */
			Assert(P_ISLEAF(opaque) && !P_IGNORE(opaque));
			if (!_bt_mark_page_halfdead(rel, vstate->info->heaprel, leafbuf,
										stack))
			{
				_bt_relbuf(rel, leafbuf);
				return;
			}
		}

		/*
		 * Then unlink it from its siblings.  Each call to
		 * _bt_unlink_halfdead_page unlinks the topmost page from the subtree,
		 * making it shallower.  Iterate until the leafbuf page is deleted.
		 */
		rightsib_empty = false;
```
## Detailed Description
_bt_pagedel is the main entry point for B-tree leaf page deletion, implementing a complete two-phase deletion process. The function first marks empty leaf pages as half-dead (removing downlinks from parent pages), then progressively unlinks pages from their siblings until the entire subtree is deleted.

The function handles several complex scenarios including:
- Detection and handling of incomplete splits that would make deletion unsafe
- Iterative deletion of right siblings when they become deletable after removing downlinks
- Cooperation with VACUUM bulk delete statistics to avoid double-counting deleted pages
- Recovery from interrupted deletion operations (pages already marked half-dead)

The algorithm maintains strict safety checks to prevent deletion of rightmost pages, root pages, non-empty pages, and pages involved in incomplete splits. It uses a search stack to locate parent pages and coordinate the hierarchical deletion process.

## Parameters
- : The B-tree index relation being modified
- : Buffer containing the target leaf page to delete (must be pinned and locked)
- : VACUUM state containing bulk delete statistics and heap relation reference

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md) (gets block number for tracking)
  - BTPageGetOpaque (accesses B-tree page metadata)
  - P_ISLEAF, P_ISDELETED, P_ISHALFDEAD (page state checks)
  - P_RIGHTMOST, P_ISROOT, P_INCOMPLETE_SPLIT (safety checks)
  - [_bt_leftsib_splitflag](_bt_leftsib_splitflag.md) (checks for incomplete split conditions)
  - [CopyIndexTuple](../C/CopyIndexTuple.md) (creates copy of high key for search)
  - [_bt_mkscankey](_bt_mkscankey.md) (creates insertion scan key)
  - [_bt_search](_bt_search.md) (finds parent page location)
  - [_bt_mark_page_halfdead](_bt_mark_page_halfdead.md) (first phase: marks page and removes downlinks)
  - [_bt_unlink_halfdead_page](_bt_unlink_halfdead_page.md) (second phase: unlinks pages from siblings)
  - [_bt_getbuf](_bt_getbuf.md), _bt_relbuf, _bt_lockbuf, _bt_unlockbuf (buffer management)
- Called from:
  - [btvacuumpage](btvacuumpage.md) (main VACUUM page processing loop)

## Notes and Other Information
- Implements complete page deletion with two distinct phases for crash recovery
- Can delete multiple adjacent empty pages in a single call by following right siblings
- Maintains VACUUM bulk delete statistics cooperation to avoid double-counting
- Uses temporary memory context due to memory leakage during complex operations
- Includes extensive safety checks to prevent deletion in unsafe conditions
- Handles legacy half-dead internal pages from pre-9.4 PostgreSQL versions
- The function may iterate multiple times when processing chains of deletable siblings
- Drops and reacquires locks strategically to avoid deadlocks during parent page searches