# _bt_binsrch_insert

## Location
[src/backend/access/nbtree/nbtsearch.c:468-595](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsearch.c#L468-L595)

## Overview
This function performs cacheable, incremental binary search on leaf pages during insertion operations, supporting bound caching for improved performance on repeated searches.

## Definition

```c
OffsetNumber
_bt_binsrch_insert(Relation rel, BTInsertState insertstate)
```
## Detailed Description
_bt_binsrch_insert is a specialized binary search function optimized for insertion operations on B-tree leaf pages. Unlike the general _bt_binsrch function, it supports caching of search bounds between calls, which can significantly improve performance when multiple searches are performed on the same page.

The function maintains cached low and stricthigh bounds in the insertstate structure, allowing subsequent calls to reuse previous search results rather than starting from scratch. It also handles posting list tuple matches by setting the postingoff field when a tuple needs to be split due to overlapping TID ranges.

The function includes comprehensive error checking for index corruption, particularly when duplicate table TIDs are encountered, which should never happen in a correctly functioning index.

## Parameters / Member Variables
- `rel`: The B-tree index relation
- `insertstate`: BTInsertState structure containing insertion context, including the target buffer, search key, cached bounds, and posting list information
## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md)
  - BTPageGetOpaque
  - P_ISLEAF
  - P_FIRSTDATAKEY
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - [_bt_compare](_bt_compare.md)
  - [_bt_binsrch_posting](_bt_binsrch_posting.md)
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)
  - [ItemPointerGetOffsetNumber](../I/ItemPointerGetOffsetNumber.md)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
- Called from (representative examples):
  - [_bt_check_unique](_bt_check_unique.md)
  - [_bt_findinsertloc](_bt_findinsertloc.md)

## Notes and Other Information
Key features and behaviors:
1. **Bound caching**: Maintains low and stricthigh bounds between calls for performance optimization
2. **Leaf page only**: Exclusively operates on leaf pages (enforced by assertion)
3. **Insert semantics**: Always uses nextkey=false semantics (searching for >= key)
4. **Posting list handling**: Detects and handles posting list tuple matches via postingoff field
5. **Corruption detection**: Includes error checking for duplicate table TIDs indicating index corruption
6. **Incremental search**: Can resume from cached bounds rather than full binary search
7. **Bound validation**: Manages bounds_valid flag to ensure cache coherency
8. **Edge case handling**: Properly handles empty pages and out-of-bounds results

The cached bounds approach is particularly beneficial during operations that perform multiple searches on the same page, such as uniqueness checking or complex insertion positioning. Callers are responsible for invalidating bounds when they modify the page structure.