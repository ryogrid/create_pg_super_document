# PruneFreezeResult

## Location
[src/include/access/heapam.h:226-264](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/heapam.h#L226-L264)

## Overview
PruneFreezeResult is a structure that contains per-page state information returned by heap_page_prune_and_freeze(), providing comprehensive statistics and status about tuple pruning and freezing operations performed on a heap page.

## Definition

```c
typedef struct PruneFreezeResult
{
	int			ndeleted;		/* Number of tuples deleted from the page */
	int			nnewlpdead;		/* Number of newly LP_DEAD items */
	int			nfrozen;		/* Number of tuples we froze */

	/* Number of live and recently dead tuples on the page, after pruning */
	int			live_tuples;
	int			recently_dead_tuples;

	/*
	 * all_visible and all_frozen indicate if the all-visible and all-frozen
	 * bits in the visibility map can be set for this page, after pruning.
	 *
	 * vm_conflict_horizon is the newest xmin of live tuples on the page.  The
	 * caller can use it as the conflict horizon when setting the VM bits.  It
	 * is only valid if we froze some tuples (nfrozen > 0), and all_frozen is
	 * true.
	 *
	 * These are only set if the HEAP_PRUNE_FREEZE option is set.
	 */
	bool		all_visible;
	bool		all_frozen;
	TransactionId vm_conflict_horizon;

	/*
	 * Whether or not the page makes rel truncation unsafe.  This is set to
	 * 'true', even if the page contains LP_DEAD items.  VACUUM will remove
	 * them before attempting to truncate.
	 */
	bool		hastup;

	/*
	 * LP_DEAD items on the page after pruning.  Includes existing LP_DEAD
	 * items.
	 */
	int			lpdead_items;
	OffsetNumber deadoffsets[MaxHeapTuplesPerPage];
} PruneFreezeResult;
```
## Detailed Description
PruneFreezeResult serves as a comprehensive reporting mechanism for heap page pruning and freezing operations. This structure captures detailed statistics about what was accomplished during a single heap_page_prune_and_freeze() operation, including counts of deleted tuples, newly dead items, frozen tuples, and the overall state of live and recently dead tuples remaining on the page.

A critical aspect of this structure is its role in visibility map management. The all_visible and all_frozen flags indicate whether the corresponding bits can be safely set in the visibility map after the pruning operation. The vm_conflict_horizon field provides the newest xmin of live tuples on the page, which serves as the conflict horizon when setting visibility map bits. This field is only valid when tuples were actually frozen (nfrozen > 0) and the page is entirely frozen (all_frozen is true).

The structure also tracks information relevant to relation truncation safety through the hastup flag, which indicates whether the page contains tuples that would make truncation unsafe. Even pages with LP_DEAD items set this flag to true, as VACUUM will remove these dead items before attempting truncation operations.

## Parameters / Member Variables
- : Count of tuples that were deleted from the page during the operation
- : Count of items that became LP_DEAD during this operation (newly dead)
- : Count of tuples that were frozen during the operation
- : Count of live tuples remaining on the page after pruning
- : Count of recently dead tuples remaining on the page after pruning
- : Boolean indicating if the all-visible bit can be set in the visibility map
- : Boolean indicating if the all-frozen bit can be set in the visibility map
- : Newest xmin of live tuples, used as conflict horizon for VM bits (valid only when nfrozen > 0 and all_frozen is true)
- : Boolean indicating whether the page makes relation truncation unsafe
- : Total count of LP_DEAD items on the page (including pre-existing ones)
- : Array of offset numbers for all LP_DEAD items on the page

## Dependencies
- Functions called/Symbols referenced:
  - MaxHeapTuplesPerPage
  - TransactionId
  - OffsetNumber
- Called from (representative examples):
  - [heap_page_prune_opt](../h/heap_page_prune_opt.md) (src/backend/access/heap/pruneheap.c:256)
  - [heap_page_prune_and_freeze](../h/heap_page_prune_and_freeze.md) (src/backend/access/heap/pruneheap.c:354)
  - [lazy_scan_prune](../l/lazy_scan_prune.md) (src/backend/access/heap/vacuumlazy.c:1419)

## Notes and Other Information
The visibility map related fields (all_visible, all_frozen, vm_conflict_horizon) are only populated when the HEAP_PRUNE_FREEZE option is set during the pruning operation. The deadoffsets array can accommodate up to MaxHeapTuplesPerPage entries, representing the maximum possible number of dead items on a single heap page. This structure is essential for VACUUM's decision-making process regarding visibility map updates and page-level optimizations, providing the detailed accounting necessary for safe and efficient heap maintenance operations.