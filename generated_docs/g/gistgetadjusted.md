# gistgetadjusted

## Location
[src/backend/access/gist/gistutil.c:315-372](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistutil.c#L315-L372)

## Overview
The `gistgetadjusted` function creates a union of two GiST index tuples and returns a new adjusted tuple only if the union differs from the original tuple.

## Definition
```c
IndexTuple gistgetadjusted(Relation r, IndexTuple oldtup, IndexTuple addtup, GISTSTATE *giststate)
```

## Detailed Description
This function performs a crucial operation in GiST index maintenance by computing the union of two index tuples and determining if an adjustment is needed. It decompresses both input tuples, computes the union of their key attributes using the appropriate union operators, and creates a new tuple only if the union result differs from the original tuple.

The function optimizes by returning NULL when no adjustment is needed (i.e., when the union equals the old tuple), avoiding unnecessary tuple creation and storage. This is particularly important during index insertions and splits where parent nodes may need to be adjusted to accommodate new or modified child nodes.

The union operation respects NULL semantics - the union of keys may be NULL if and only if both input keys are NULL.

## Parameters / Member Variables
- `r`: The GiST index relation being processed
- `oldtup`: The existing IndexTuple that may need adjustment
- `addtup`: The new IndexTuple to be combined with the old tuple
- `giststate`: Pointer to GISTSTATE containing operator information for the index

## Dependencies
- Functions called/Symbols referenced:
  - gistDeCompressAtt
  - IndexRelationGetNumberOfKeyAttributes
  - gistMakeUnionKey
  - gistKeyIsEQ
  - gistFormTuple
- Called from (representative examples):
  - gistdoinsert
  - gistformdownlink
  - gistProcessItup
  - gistRelocateBuildBuffersOnSplit

## Notes and Other Information
- Returns NULL if no adjustment is needed (optimization to avoid unnecessary tuple creation)
- The returned tuple (if any) inherits the ItemPointer from the old tuple
- Uses INDEX_MAX_KEYS arrays to handle attribute processing
- Implements early termination optimization - once neednew is set to true, attribute checking continues but equality checks are skipped
- The function handles NULL attributes correctly according to GiST semantics
- Critical for maintaining GiST index structure during insertions and node splits