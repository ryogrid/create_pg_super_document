# BTPageIsRecyclable

## Location
[src/include/access/nbtree.h:291-323](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/nbtree.h#L291-L323)

## Overview
BTPageIsRecyclable is a static inline function that determines whether a deleted B-tree page can be safely recycled and reused for new data.

## Definition
```c
static inline bool
BTPageIsRecyclable(Page page, Relation heaprel)
```

## Detailed Description
This function centralizes the policy for determining when deleted B-tree pages are safe to reuse. It checks if a page is deleted and whether its safe transaction ID is old enough that no ongoing transactions could still reference it. The function ensures MVCC compliance by preventing premature recycling of pages that might still be visible to concurrent scans.

The function performs visibility checks using the global visibility map to determine if the deletion transaction ID is old enough that no active transaction could have seen the downlink to this page. Only when this condition is met can the page be safely recycled.

## Parameters / Member Variables
- `page`: The B-tree page to check for recyclability (must not be a new page)
- `heaprel`: The heap relation associated with the B-tree index, used for visibility checking

## Dependencies
- Functions called/Symbols referenced:
  - BTPageGetOpaque
  - P_ISDELETED
  - [BTPageGetDeleteXid](BTPageGetDeleteXid.md)
  - [GlobalVisCheckRemovableFullXid](../G/GlobalVisCheckRemovableFullXid.md)
  - [PageIsNew](../P/PageIsNew.md) (in assertions)
  - BTPageOpaque (type)
  - FullTransactionId (type)
- Called from (representative examples):
  - [_bt_allocbuf](../b/_bt_allocbuf.md)
  - [btvacuumpage](../b/btvacuumpage.md)

## Notes and Other Information
This function is critical for B-tree space management and MVCC correctness. It includes detailed comments explaining the tombstone concept - deleted pages must remain as tombstones until no concurrent scans could reference them. The function explicitly excludes new pages (PageIsNew) which must be handled separately by callers. The logic is intentionally duplicated in _bt_pendingfsm_finalize() for performance reasons when working without direct page access. The function serves as the authoritative policy for page recycling decisions throughout the B-tree subsystem.