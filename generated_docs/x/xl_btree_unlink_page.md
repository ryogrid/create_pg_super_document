# xl_btree_unlink_page

## Location
[src/include/access/nbtxlog.h:310-329](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/nbtxlog.h#L310-L329)

## Overview
WAL record structure for the second stage of B-tree page deletion, permanently unlinking a page from its siblings and marking it as deleted tombstone for safe reclamation.

## Definition

```c
typedef struct xl_btree_unlink_page
{
	BlockNumber leftsib;		/* target block's left sibling, if any */
	BlockNumber rightsib;		/* target block's right sibling */
	uint32		level;			/* target block's level */
	FullTransactionId safexid;	/* target block's BTPageSetDeleted() XID */

	/*
	 * Information needed to recreate a half-dead leaf page with correct
	 * topparent link.  The fields are only used when deletion operation's
	 * target page is an internal page.  REDO routine creates half-dead page
	 * from scratch to keep things simple (this is the same convenient
	 * approach used for the target page itself).
	 */
	BlockNumber leafleftsib;
	BlockNumber leafrightsib;
	BlockNumber leaftopparent;	/* next child down in the subtree */

	/* xl_btree_metadata FOLLOWS IF XLOG_BTREE_UNLINK_PAGE_META */
} xl_btree_unlink_page;
```
## Detailed Description
The xl_btree_unlink_page structure represents the second and final phase of B-tree page deletion. After a page has been marked half-dead using xl_btree_mark_page_halfdead, this operation permanently unlinks the target page from its siblings and marks it as a deleted tombstone. The structure supports deletion of both leaf pages and internal pages, with special handling for leaf page recreation when deleting internal nodes in a subtree.

The deletion process updates sibling links to bypass the target page, marks the target page as deleted with a safe transaction ID for later reclamation, and may update the fast root if the right sibling becomes the only remaining page at its level. When deleting internal pages, the structure includes information to recreate the associated half-dead leaf page with correct parent links.

## Parameters / Member Variables
- `leftsib`: Block number of the target page's left sibling (P_NONE if none)
- `rightsib`: Block number of the target page's right sibling
- `level`: Level of the target page being deleted (0 for leaf pages)
- `safexid`: Full transaction ID marking when the deleted page can be safely recycled
- `leafleftsib`: Left sibling of the associated leaf page (used only for internal page deletion)
- `leafrightsib`: Right sibling of the associated leaf page (used only for internal page deletion)
- `leaftopparent`: Next child page down in the subtree (used only for internal page deletion, InvalidBlockNumber if not applicable)
## Dependencies
- Functions called/Symbols referenced:
  - FullTransactionId (transaction ID type)
  - SizeOfBtreeUnlinkPage (size calculation macro)
  - xl_btree_metadata (optional trailing metadata)
- Called from (representative examples):
  - [_bt_unlink_halfdead_page](../b/_bt_unlink_halfdead_page.md) (src/backend/access/nbtree/nbtpage.c:2672)
  - [btree_xlog_unlink_page](../b/btree_xlog_unlink_page.md) (src/backend/access/nbtree/nbtxlog.c:801)
  - [btree_desc](../b/btree_desc.md) (src/backend/access/rmgrdesc/nbtdesc.c:95)

## Notes and Other Information
- This is the second stage of page deletion, following xl_btree_mark_page_halfdead
- The target page is reinitialized as an empty deleted page during recovery rather than preserving original content
- May include up to 5 backup blocks: target, left sibling, right sibling, leaf page, and metapage
- When XLOG_BTREE_UNLINK_PAGE_META variant is used, xl_btree_metadata follows this structure
- The safexid field uses ReadNextFullTransactionId() to ensure safe page reclamation timing
- Leaf page fields (leafleftsib, leafrightsib, leaftopparent) are only meaningful when target is an internal page
- Fast root optimization may update the metapage if the right sibling becomes the sole remaining page at its level