# GISTSTATE

## Location
[src/include/access/gist_private.h:75-98](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/gist_private.h#L75-L98)

## Overview
GISTSTATE is a core data structure that maintains all the information needed for any GiST (Generalized Search Tree) index operation, including opclass-specific support functions and tuple descriptors.

## Definition

```c
typedef struct GISTSTATE
{
	MemoryContext scanCxt;		/* context for scan-lifespan data */
	MemoryContext tempCxt;		/* short-term context for calling functions */

	TupleDesc	leafTupdesc;	/* index's tuple descriptor */
	TupleDesc	nonLeafTupdesc; /* truncated tuple descriptor for non-leaf
								 * pages */
	TupleDesc	fetchTupdesc;	/* tuple descriptor for tuples returned in an
								 * index-only scan */

	FmgrInfo	consistentFn[INDEX_MAX_KEYS];
	FmgrInfo	unionFn[INDEX_MAX_KEYS];
	FmgrInfo	compressFn[INDEX_MAX_KEYS];
	FmgrInfo	decompressFn[INDEX_MAX_KEYS];
	FmgrInfo	penaltyFn[INDEX_MAX_KEYS];
	FmgrInfo	picksplitFn[INDEX_MAX_KEYS];
	FmgrInfo	equalFn[INDEX_MAX_KEYS];
	FmgrInfo	distanceFn[INDEX_MAX_KEYS];
	FmgrInfo	fetchFn[INDEX_MAX_KEYS];

	/* Collations to pass to the support functions */
	Oid			supportCollation[INDEX_MAX_KEYS];
} GISTSTATE;
```
## Detailed Description
GISTSTATE serves as the central state holder for GiST index operations, encapsulating both memory management contexts and the complete set of opclass-specific support functions. The structure maintains two distinct memory contexts: scanCxt for long-lived scan data and tempCxt for short-term function calls that are typically reset after each tuple. This design enables efficient memory management during index operations while providing the necessary function dispatch mechanism for the extensible GiST framework.

The structure stores cached FmgrInfo structures for all nine standard GiST support functions across all index columns, eliminating the need for repeated function lookups during index operations. It also maintains multiple tuple descriptors to handle different contexts: leaf pages, non-leaf pages, and index-only scans.

## Parameters / Member Variables
- : Memory context for scan-lifespan data that persists throughout the index operation
- : Short-term memory context for calling functions, typically reset after each tuple
- : Tuple descriptor for the complete index tuples stored in leaf pages
- : Truncated tuple descriptor optimized for non-leaf internal pages
- : Tuple descriptor for tuples returned during index-only scan operations
- : Cached function info for consistency check functions per column
- : Cached function info for union operation functions per column
- : Cached function info for compression functions per column
- : Cached function info for decompression functions per column
- : Cached function info for penalty calculation functions per column
- : Cached function info for page split decision functions per column
- : Cached function info for equality test functions per column
- : Cached function info for distance calculation functions per column
- : Cached function info for fetch operation functions per column
- : Collation information to pass to support functions per column

## Dependencies
- Functions called/Symbols referenced:
  - INDEX_MAX_KEYS
  - [MemoryContext](../M/MemoryContext.md)
  - [TupleDesc](../T/TupleDesc.md)
  - [FmgrInfo](../F/FmgrInfo.md)
  - Oid
- Called from (representative examples):
  - [initGISTstate](../i/initGISTstate.md)
  - [freeGISTstate](../f/freeGISTstate.md)
  - [gistinsert](../g/gistinsert.md)
  - [gistbeginscan](../g/gistbeginscan.md)
  - [gistScanPage](../g/gistScanPage.md)

## Notes and Other Information
The GISTSTATE structure is typically initialized once per index operation via initGISTstate() and freed with freeGISTstate(). The cached function information significantly improves performance by avoiding repeated catalog lookups for support functions. The dual memory context design (scanCxt vs tempCxt) provides flexibility in memory management strategies depending on the operation's requirements and can be configured to use the same context when per-tuple resets are not needed.