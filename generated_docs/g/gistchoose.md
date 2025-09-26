# gistchoose

## Location
[src/backend/access/gist/gistutil.c:373-545](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistutil.c#L373-L545)

## Overview
The `gistchoose` function searches an upper index page to find the entry with the lowest penalty for insertion of a new index key, implementing the GiST choose algorithm.

## Definition
```c
OffsetNumber gistchoose(Relation r, Page p, IndexTuple it, GISTSTATE *giststate)
```

## Detailed Description
This function implements the core GiST choose algorithm for navigating down the index tree during insertion. It evaluates each tuple on a non-leaf page to determine which subtree would incur the lowest penalty when the new tuple is inserted. The function uses a multi-column penalty comparison approach where earlier columns in the index definition have strictly higher importance than later columns.

The algorithm includes sophisticated optimizations for handling ties: when multiple tuples have identical penalties, it uses randomization to balance between cache-friendly behavior (preferring the same path) and space utilization (distributing inserts evenly). The function also implements early termination when it finds a tuple with zero penalty across all columns.

The penalty calculation respects the hierarchical nature of multi-column GiST indexes, ensuring that a better penalty in an earlier column always trumps any penalty in later columns.

## Parameters / Member Variables
- `r`: The GiST index relation being searched
- `p`: The non-leaf page to search through (must not be a leaf page)
- `it`: The IndexTuple to be inserted (contains compressed entry data)
- `giststate`: Pointer to GISTSTATE containing operator information for penalty calculations

## Dependencies
- Functions called/Symbols referenced:
  - GistPageIsLeaf
  - [gistDeCompressAtt](gistDeCompressAtt.md)
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - [PageGetItem](../P/PageGetItem.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - IndexRelationGetNumberOfKeyAttributes
  - [index_getattr](../i/index_getattr.md)
  - [gistdentryinit](gistdentryinit.md)
  - [gistpenalty](gistpenalty.md)
  - [pg_prng_bool](../p/pg_prng_bool.md)
- Called from (representative examples):
  - [gistdoinsert](gistdoinsert.md)
  - [gistProcessItup](gistProcessItup.md)

## Notes and Other Information
- Only operates on non-leaf pages (asserted at function entry)
- Returns FirstOffsetNumber if page is empty (shouldn't happen in normal operation)
- Uses INDEX_MAX_KEYS arrays for handling multi-column penalties
- Implements lexicographic penalty comparison: penalties in earlier columns are infinitely more important than later columns
- Includes sophisticated tie-breaking logic using randomization to balance cache efficiency and space utilization
- Supports early termination optimization when zero penalty is found across all columns
- The best_penalty array uses -1 to indicate unexamined columns, with undefined values to the right of the first -1
- Critical component of GiST insertion performance, determining the traversal path down the index tree