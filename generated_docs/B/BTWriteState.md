# BTWriteState

## Location
[src/backend/access/nbtree/nbtsort.c:244-251](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsort.c#L244-L251)

## Overview
BTWriteState is the overall status record that manages the index writing phase during B-tree index construction, coordinating page allocation and bulk writing operations.

## Definition


## Detailed Description
BTWriteState serves as the central coordination structure for the index writing phase of B-tree construction. It manages the relationship between the source heap relation and target index relation, coordinates bulk write operations through the bulk write state, and tracks resource allocation during the build process.

The structure provides a generic insertion scankey that can be reused throughout the writing process for efficiency, avoiding repeated scankey construction. It also maintains a count of allocated pages to track storage resource usage during index construction.

## Parameters / Member Variables
- : Relation pointer to the source heap table being indexed
- : Relation pointer to the target B-tree index being constructed
- : Pointer to BulkWriteState managing efficient bulk write operations
- : BTScanInsert structure providing a generic insertion scankey that can be reused for tuple insertions
- : BlockNumber tracking the total number of pages that have been allocated for the index

## Dependencies
- Functions called/Symbols referenced:
  - [BulkWriteState](BulkWriteState.md)
  - BTScanInsert
- Called from (representative examples):
  - [_bt_leafbuild](../b/_bt_leafbuild.md)
  - [_bt_blnewpage](../b/_bt_blnewpage.md)
  - [_bt_blwritepage](../b/_bt_blwritepage.md)
  - [_bt_pagestate](../b/_bt_pagestate.md)
  - _bt_buildadd
  - _bt_sort_dedup_finish_pending
  - _bt_uppershutdown
  - _bt_load

## Notes and Other Information
BTWriteState is specifically designed for the writing phase of index construction and provides centralized management of bulk write operations. The structure maintains references to both source and target relations to coordinate data movement during index building. The generic insertion scankey (inskey) is an optimization that avoids repeatedly constructing scankeys for tuple insertion operations. The page allocation counter (btws_pages_alloced) helps track storage resource usage and can be used for monitoring and debugging index construction progress.