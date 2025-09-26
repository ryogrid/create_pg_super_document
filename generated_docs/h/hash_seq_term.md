# hash_seq_term

## Location
[src/backend/utils/hash/dynahash.c:1474-1493](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/hash/dynahash.c#L1474-L1493)

## Overview
Terminates a hash table sequential scan by deregistering it from the hash table's scan tracking system.

## Definition

```c
void
hash_seq_term(HASH_SEQ_STATUS *status)
```
## Detailed Description
The hash_seq_term function is responsible for properly cleaning up after a sequential scan of a hash table. It checks if the hash table is frozen (immutable), and if not, deregisters the sequential scan from the hash table's internal scan tracking mechanism. This is essential for maintaining consistency during hash table operations and preventing memory leaks or corruption during concurrent access.

## Parameters / Member Variables
- : Pointer to a HASH_SEQ_STATUS structure that contains the state information for the sequential scan being terminated

## Dependencies
- Functions called/Symbols referenced:
  - [deregister_seq_scan](../d/deregister_seq_scan.md)
  - [HASH_SEQ_STATUS](../H/HASH_SEQ_STATUS.md) (structure access)
- Called from (representative examples):
  - [logicalrep_relmap_invalidate_cb](../l/logicalrep_relmap_invalidate_cb.md)
  - [logicalrep_partmap_invalidate_cb](../l/logicalrep_partmap_invalidate_cb.md)  
  - [hash_seq_search](hash_seq_search.md)
  - [PortalHashTableDeleteAll](../P/PortalHashTableDeleteAll.md)
  - [PreCommit_Portals](../P/PreCommit_Portals.md)

## Notes and Other Information
- This function should always be called to properly terminate hash table sequential scans
- The frozen check prevents deregistration attempts on immutable hash tables
- Part of the PostgreSQL dynamic hash table implementation in dynahash.c
- Essential for proper resource cleanup and maintaining hash table consistency