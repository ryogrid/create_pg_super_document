# gistindex_keytest

## Location
[src/backend/access/gist/gistget.c:125-327](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistget.c#L125-L327)

## Overview
gistindex_keytest evaluates whether an index tuple satisfies the scan conditions by testing it against scan keys using the Consistent and Distance functions of the GiST operator classes.

## Definition
static bool gistindex_keytest(IndexScanDesc scan, IndexTuple tuple, Page page, OffsetNumber offset, bool *recheck_p, bool *recheck_distances_p)

## Detailed Description
This function is the core predicate evaluation mechanism for GiST index scans. It performs two main operations:

1. **Consistency Testing**: For each scan key, it calls the appropriate Consistent function from the operator class to determine if the tuple satisfies the search condition. The function handles NULL values according to GiST semantics where union(VAL, NULL) = VAL for non-leaf pages.

2. **Distance Calculation**: For ordered scans, it computes distances using Distance functions from the operator class, storing results in the scan opaque data.

The function handles special cases like invalid tuples from pre-9.1 installations by treating them as matches with minimum distances. It also manages recheck flags that indicate whether the heap tuple needs to be re-examined or distances recalculated for exact results.

## Parameters / Member Variables
- `scan`: IndexScanDesc containing scan state and keys
- `tuple`: IndexTuple being evaluated 
- `page`: Page containing the tuple
- `offset`: Offset of the tuple within the page
- `recheck_p`: Output parameter indicating if heap tuple recheck is needed
- `recheck_distances_p`: Output parameter indicating if distance recalculation is needed

## Dependencies
- Functions called/Symbols referenced:
  - GistTupleIsInvalid
  - GistPageIsLeaf
  - get_float8_infinity
  - [index_getattr](../i/index_getattr.md)
  - gistdentryinit
  - [FunctionCall5Coll](../F/FunctionCall5Coll.md)
  - [DatumGetFloat8](../D/DatumGetFloat8.md)
- Called from:
  - [gistScanPage](gistScanPage.md)

## Notes and Other Information
- This is a static function only accessible within gistget.c
- Operates in a short-lived memory context so no explicit cleanup is needed
- Handles both leaf and non-leaf pages with different semantics
- Invalid tuples are treated as matches to ensure compatibility with pre-9.1 indexes
- Recheck flags allow for lossy index operations that require heap verification
- Distance functions were enhanced in version 9.5 to support recheck flags