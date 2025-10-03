# RelationGetIndexScan

## Location
[src/backend/access/index/genam.c:80-143](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/index/genam.c#L80-L143)

## Overview
Creates and initializes an IndexScanDesc structure that serves as the foundation for all indexed access method scans in PostgreSQL.

## Definition

```c
IndexScanDesc
RelationGetIndexScan(Relation indexRelation, int nkeys, int norderbys)
```
## Detailed Description
RelationGetIndexScan is a critical function in PostgreSQL's index access infrastructure that creates and populates an IndexScanDesc structure. This function serves as the standard entry point for all access methods (AMs) to initialize their scan operations. The function allocates memory for the scan descriptor and sets up initial state including workspace for scan keys and order-by conditions. It handles special considerations for recovery scenarios by setting appropriate flags for tuple visibility checking.

The function is designed to be called by AM-specific beginscan routines, which then perform their own locking and additional initialization. This design provides a consistent interface across all index access methods while allowing each AM to customize behavior as needed.

## Parameters / Member Variables
- `indexRelation`: The index relation that will be scanned
- `nkeys`: Number of scan keys (index qualification conditions) that will be used
- `norderbys`: Number of index order-by operators for ordered scans
## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (memory allocation)
  - [TransactionStartedDuringRecovery](../T/TransactionStartedDuringRecovery.md) (recovery state checking)
  - InvalidSnapshot (snapshot initialization)
- Called from (representative examples):
  - [brinbeginscan](../b/brinbeginscan.md) (BRIN index access method)
  - [ginbeginscan](../g/ginbeginscan.md) (GIN index access method)
  - [gistbeginscan](../g/gistbeginscan.md) (GiST index access method)
  - [hashbeginscan](../h/hashbeginscan.md) (Hash index access method)
  - [btbeginscan](../b/btbeginscan.md) (B-tree index access method)
  - [spgbeginscan](../s/spgbeginscan.md) (SP-GiST index access method)

## Notes and Other Information
- The function allocates workspace for scan keys but does not populate them - this occurs later during amrescan
- During recovery, the function sets ignore_killed_tuples to false to ensure proper MVCC behavior on standby nodes
- The heapRelation field is initially set to NULL and may be populated later by the calling AM
- The xs_snapshot field must be initialized by the caller before use
- This function is part of the general access method interface and must be used by all index AMs - they cannot bypass it in their beginscan implementations