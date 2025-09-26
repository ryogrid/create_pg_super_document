# SpGistScanOpaqueData

## Location
src/include/access/spgist_private.h: 189 - 243

## Overview
SpGistScanOpaqueData is the comprehensive private state structure for SP-GiST index scans, containing all necessary information to manage scan operations, queue processing, and result handling.

## Definition

```c
typedef struct SpGistScanOpaqueData
{
	SpGistState state;			/* see above */
	pairingheap *scanQueue;		/* queue of to be visited items */
	MemoryContext tempCxt;		/* short-lived memory context */
	MemoryContext traversalCxt; /* single scan lifetime memory context */

	/* Control flags showing whether to search nulls and/or non-nulls */
	bool		searchNulls;	/* scan matches (all) null entries */
	bool		searchNonNulls; /* scan matches (some) non-null entries */

	/* Index quals to be passed to opclass (null-related quals removed) */
	int			numberOfKeys;	/* number of index qualifier conditions */
	ScanKey		keyData;		/* array of index qualifier descriptors */
	int			numberOfOrderBys;	/* number of ordering operators */
	int			numberOfNonNullOrderBys;	/* number of ordering operators
											 * with non-NULL arguments */
	ScanKey		orderByData;	/* array of ordering op descriptors */
	Oid		   *orderByTypes;	/* array of ordering op return types */
	int		   *nonNullOrderByOffsets;	/* array of offset of non-NULL
										 * ordering keys in the original array */
	Oid			indexCollation; /* collation of index column */

	/* Opclass defined functions: */
	FmgrInfo	innerConsistentFn;
	FmgrInfo	leafConsistentFn;

	/* Pre-allocated workspace arrays: */
	double	   *zeroDistances;
	double	   *infDistances;

	/* These fields are only used in amgetbitmap scans: */
	TIDBitmap  *tbm;			/* bitmap being filled */
	int64		ntids;			/* number of TIDs passed to bitmap */

	/* These fields are only used in amgettuple scans: */
	bool		want_itup;		/* are we reconstructing tuples? */
	TupleDesc	reconTupDesc;	/* if so, descriptor for reconstructed tuples */
	int			nPtrs;			/* number of TIDs found on current page */
	int			iPtr;			/* index for scanning through same */
	ItemPointerData heapPtrs[MaxIndexTuplesPerPage];	/* TIDs from cur page */
	bool		recheck[MaxIndexTuplesPerPage]; /* their recheck flags */
	bool		recheckDistances[MaxIndexTuplesPerPage];	/* distance recheck
															 * flags */
	HeapTuple	reconTups[MaxIndexTuplesPerPage];	/* reconstructed tuples */

	/* distances (for recheck) */
	IndexOrderByDistance *distances[MaxIndexTuplesPerPage];

	/*
	 * Note: using MaxIndexTuplesPerPage above is a bit hokey since
	 * SpGistLeafTuples aren't exactly IndexTuples; however, they are larger,
	 * so this is safe.
	 */
} SpGistScanOpaqueData;
```
## Detailed Description
SpGistScanOpaqueData serves as the comprehensive control structure for SP-GiST index scan operations. It manages the entire lifecycle of a scan, from initialization through result delivery. The structure supports both tuple-returning (amgettuple) and bitmap (amgetbitmap) scan modes, with specialized fields for each operation type.

The design accommodates SP-GiST's sophisticated search algorithms including priority-queue based traversal, null handling, distance-ordered searches, and result reconstruction. It maintains proper memory management through dedicated contexts and supports complex operator class interactions through cached function pointers. The structure also handles the intricacies of index-only scans where tuples may need to be reconstructed from index data.

## Parameters / Member Variables
- : SpGistState containing index and type information
- : Pairing heap for managing search items in priority order
- : Short-lived memory context for temporary allocations during scan
- : Memory context lasting for the entire scan lifetime
- : Flag indicating whether to search NULL entries
- : Flag indicating whether to search non-NULL entries
- : Count of index qualification conditions
- : Array of scan key descriptors for index qualifications
- : Count of ordering operators for ordered scans
- : Count of non-NULL ordering operators
- : Array of ordering operation descriptors
- : Array of return types for ordering operations
- : Offset mapping for non-NULL ordering keys
- : Collation specification for the index column
- : Cached function pointer for inner node consistency checking
- : Cached function pointer for leaf node consistency checking
- : Pre-allocated array of zero distances for optimization
- : Pre-allocated array of infinite distances for optimization
- : TID bitmap for amgetbitmap scan operations
- : Count of TIDs added to the bitmap
- : Flag indicating whether to reconstruct tuples for index-only scans
- : Tuple descriptor for reconstructed tuples
- : Number of TIDs found on current page
- : Current index for iterating through TIDs
- : Array of heap tuple pointers from current page
- : Array of recheck flags for qualification conditions
- : Array of distance recheck flags
- : Array of reconstructed heap tuples
- : Array of distance calculation results for ordering

## Dependencies
- Functions called/Symbols referenced:
  - SpGistState (index state management)
  - pairingheap (priority queue implementation)
  - MemoryContext (memory management)
  - ScanKey (scan key descriptors)
  - TIDBitmap (bitmap scan support)
  - FmgrInfo (function manager)
  - MaxIndexTuplesPerPage (page capacity constant)

- Called from (representative examples):
  - spgbeginscan (spgscan.c:312)
  - SpGistScanOpaque (spgist_private.h:245)

## Notes and Other Information
- Central structure for all SP-GiST scan operations, supporting both amgettuple and amgetbitmap interfaces
- Efficiently manages memory through dedicated contexts for different allocation lifetimes
- Supports complex distance-ordered queries with pre-allocated workspace arrays
- Handles both simple index scans and sophisticated index-only scans with tuple reconstruction
- The MaxIndexTuplesPerPage arrays provide efficient batching of results from individual pages
- Note indicates the use of MaxIndexTuplesPerPage is somewhat approximate since SpGistLeafTuples are larger than regular IndexTuples, but this provides a safe upper bound
- Critical for maintaining scan state across multiple page visits and complex tree traversals