# PredicateLockPageCombine

## Location
[src/backend/storage/lmgr/predicate.c:3219-3240](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L3219-L3240)

## Overview
Handles predicate lock management during page combine operations by delegating to PredicateLockPageSplit to maintain serializable isolation guarantees.

## Definition
```c
void PredicateLockPageCombine(Relation relation, BlockNumber oldblkno, BlockNumber newblkno)
```

## Detailed Description
PredicateLockPageCombine manages predicate locks when two pages are being combined in PostgreSQL's storage system. While conceptually different from page splits, this function currently implements page combines using the same mechanism as page splits due to implementation constraints.

The function ideally would remove locks from the old page after transferring them to the new page, but this approach is not feasible because other backends' local lock hash tables cannot be edited remotely. Therefore, the implementation duplicates locks to both pages rather than moving them, which may result in some false positives for serialization conflict detection but maintains correctness.

The function ensures that all serializable transactions continue to detect potential conflicts correctly even when pages are combined, which is essential for maintaining the ACID properties of serializable transactions.

## Parameters / Member Variables
- `relation`: The relation containing the pages being combined
- `oldblkno`: Block number of the page being combined/removed
- `newblkno`: Block number of the target page receiving the combined content

## Dependencies
- Functions called/Symbols referenced:
  - [PredicateLockPageSplit](PredicateLockPageSplit.md)
- Called from (representative examples):
  - [ginDeletePage](../g/ginDeletePage.md) (GIN index page deletion)
  - [_bt_mark_page_halfdead](../b/_bt_mark_page_halfdead.md) (B-tree page deletion)

## Notes and Other Information
- This function affects ALL serializable transactions, regardless of the isolation level of the transaction performing the page combine
- The current implementation is a simplification that reuses PredicateLockPageSplit logic due to the complexity of safely removing locks from other backends' local hash tables
- May lead to false positives in serialization conflict detection, but these should be rare in practice
- The design prioritizes correctness over optimal performance, ensuring that no serialization conflicts are missed
- Skip processing for temporary tables and toast tables as they don't require predicate locking