# HeapPageFreeze

## Location
[src/include/access/heapam.h:178-221](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/heapam.h#L178-L221)

## Overview
HeapPageFreeze is a structure used by VACUUM to track the details of freezing all eligible tuples on a given heap page, maintaining transaction ID and multi-transaction ID tracking for both frozen and unfrozen scenarios.

## Definition

```c
typedef struct HeapPageFreeze
{
	/* Is heap_prepare_freeze_tuple caller required to freeze page? */
	bool		freeze_required;

	/*
	 * "Freeze" NewRelfrozenXid/NewRelminMxid trackers.
	 *
	 * Trackers used when heap_freeze_execute_prepared freezes, or when there
	 * are zero freeze plans for a page.  It is always valid for vacuumlazy.c
	 * to freeze any page, by definition.  This even includes pages that have
	 * no tuples with storage to consider in the first place.  That way the
	 * 'totally_frozen' results from heap_prepare_freeze_tuple can always be
	 * used in the same way, even when no freeze plans need to be executed to
	 * "freeze the page".  Only the "freeze" path needs to consider the need
	 * to set pages all-frozen in the visibility map under this scheme.
	 *
	 * When we freeze a page, we generally freeze all XIDs < OldestXmin, only
	 * leaving behind XIDs that are ineligible for freezing, if any.  And so
	 * you might wonder why these trackers are necessary at all; why should
	 * _any_ page that VACUUM freezes _ever_ be left with XIDs/MXIDs that
	 * ratchet back the top-level NewRelfrozenXid/NewRelminMxid trackers?
	 *
	 * It is useful to use a definition of "freeze the page" that does not
	 * overspecify how MultiXacts are affected.  heap_prepare_freeze_tuple
	 * generally prefers to remove Multis eagerly, but lazy processing is used
	 * in cases where laziness allows VACUUM to avoid allocating a new Multi.
	 * The "freeze the page" trackers enable this flexibility.
	 */
	TransactionId FreezePageRelfrozenXid;
	MultiXactId FreezePageRelminMxid;

	/*
	 * "No freeze" NewRelfrozenXid/NewRelminMxid trackers.
	 *
	 * These trackers are maintained in the same way as the trackers used when
	 * VACUUM scans a page that isn't cleanup locked.  Both code paths are
	 * based on the same general idea (do less work for this page during the
	 * ongoing VACUUM, at the cost of having to accept older final values).
	 */
	TransactionId NoFreezePageRelfrozenXid;
	MultiXactId NoFreezePageRelminMxid;

} HeapPageFreeze;
```
## Detailed Description
HeapPageFreeze serves as a comprehensive state tracking mechanism for VACUUM's tuple freezing operations on heap pages. The structure is designed to maintain two parallel sets of transaction tracking data - one for when the page undergoes freezing and another for when it doesn't. This dual-tracking approach allows VACUUM to make informed decisions about whether freezing is beneficial while maintaining proper transaction ID management.

The structure works in conjunction with heap_prepare_freeze_tuple calls, where each tuple with storage gets evaluated for freezing eligibility. The accumulated state across all these calls determines whether freezing the entire page is required. Beyond the basic freeze/no-freeze decision, the structure tracks the oldest extant transaction IDs and multi-transaction IDs in the table to ensure safe advancement of relfrozenxid/relminmxid values in pg_class.

A key design principle is that unfrozen XIDs or MXIDs remaining after VACUUM must have values greater than or equal to the final relfrozenxid/relminmxid values. This includes transaction IDs that persist as MultiXact members in tuple xmax fields. The "freeze page" trackers provide flexibility in MultiXact handling, allowing heap_prepare_freeze_tuple to prefer eager MultiXact removal while supporting lazy processing when it avoids allocating new MultiXacts.

## Parameters / Member Variables
- : Boolean flag indicating whether the heap_prepare_freeze_tuple caller must freeze the page
- : Transaction ID tracker used when the page undergoes freezing operations
- : Multi-transaction ID tracker used when the page undergoes freezing operations  
- : Transaction ID tracker used when the page is not frozen (maintained like pages without cleanup locks)
- : Multi-transaction ID tracker used when the page is not frozen

## Dependencies
- Functions called/Symbols referenced:
  - MultiXactId
  - TransactionId
- Called from (representative examples):
  - [FreezeMultiXactId](../F/FreezeMultiXactId.md) (src/backend/access/heap/heapam.c:6661)
  - [heap_prepare_freeze_tuple](../h/heap_prepare_freeze_tuple.md) (src/backend/access/heap/heapam.c:7011)
  - [heap_freeze_tuple](../h/heap_freeze_tuple.md) (src/backend/access/heap/heapam.c:7389)

## Notes and Other Information
The structure supports a flexible "freeze the page" definition that doesn't overspecify MultiXact handling, allowing heap_prepare_freeze_tuple to balance between eager MultiXact removal and lazy processing. When freeze_required is false after examining all tuples, the final freezing decision is delegated to vacuumlazy.c based on its own criteria. It's recommended that vacuumlazy.c avoid early freezing when it won't enable setting the target page as all-frozen in the visibility map, as this optimization provides the primary benefit of the freezing operation.