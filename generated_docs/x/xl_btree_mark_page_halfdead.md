# xl_btree_mark_page_halfdead

## Location
[src/include/access/nbtxlog.h:283-292](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/nbtxlog.h#L283-L292)

## Overview
WAL record structure for the first stage of B-tree page deletion, marking an empty leaf page as half-dead and removing its downlink from the parent while preserving the subtree structure.

## Definition

```c
typedef struct xl_btree_mark_page_halfdead
{
	OffsetNumber poffset;		/* deleted tuple id in parent page */

	/* information needed to recreate the leaf page: */
	BlockNumber leafblk;		/* leaf block ultimately being deleted */
	BlockNumber leftblk;		/* leaf block's left sibling, if any */
	BlockNumber rightblk;		/* leaf block's right sibling */
	BlockNumber topparent;		/* topmost internal page in the subtree */
} xl_btree_mark_page_halfdead;
```
## Detailed Description
The xl_btree_mark_page_halfdead structure represents the first phase of B-tree page deletion, which involves marking empty leaf pages as half-dead before actual deletion. This two-phase deletion protocol ensures transaction safety and index consistency during page removal operations. The structure contains information needed to identify the deletion target in the parent page and reconstruct the half-dead leaf page during recovery.

During this operation, the key space moves rightward as the parent page's downlink is updated to point to the right sibling, and the following pivot tuple is deleted. The leaf page is marked with the BTP_HALF_DEAD flag and contains a dummy high key pointing to the topmost parent page in the subtree being deleted. This approach differs from the Lanin and Shasha algorithm by moving key space right instead of left.

## Parameters / Member Variables
- : Offset number of the tuple being removed from the parent page (the downlink to be deleted)
- : Block number of the leaf page being marked as half-dead 
- : Block number of the leaf page's left sibling (may be invalid if no left sibling)
- : Block number of the leaf page's right sibling
- : Block number of the topmost internal page in the subtree being deleted (InvalidBlockNumber if leaf is the top parent)

## Dependencies
- Functions called/Symbols referenced:
  - SizeOfBtreeMarkPageHalfDead (size calculation macro)
  - BTP_HALF_DEAD (page flag constant)
- Called from (representative examples):
  - [_bt_mark_page_halfdead](../b/_bt_mark_page_halfdead.md) (src/backend/access/nbtree/nbtpage.c:2253)
  - [btree_xlog_mark_page_halfdead](../b/btree_xlog_mark_page_halfdead.md) (src/backend/access/nbtree/nbtxlog.c:716)
  - [btree_desc](../b/btree_desc.md) (src/backend/access/rmgrdesc/nbtdesc.c:86)

## Notes and Other Information
- This is the first stage of a two-phase page deletion protocol; the second stage uses xl_btree_unlink_page
- The leaf page content is not preserved since it's empty and gets reinitialized during recovery
- WAL record includes two backup blocks: the leaf block (block 0) and the parent block (block 1) 
- The parent page modification involves copying the right sibling's downlink over the target downlink and deleting the following pivot tuple
- Recovery recreates the leaf page with BTP_HALF_DEAD flag and a dummy high key pointing to the topmost parent
- Predicate locking is used to combine the leaf page with its right sibling for serializable isolation