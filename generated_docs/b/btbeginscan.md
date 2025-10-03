# btbeginscan

## Location
[src/backend/access/nbtree/nbtree.c:312-358](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtree.c#L312-L358)

## Overview
Initializes and starts a scan on a B-tree index, setting up the necessary data structures and workspace for index scanning operations.

## Definition

```c
IndexScanDesc
btbeginscan(Relation rel, int nkeys, int norderbys)
```
## Detailed Description
The btbeginscan function is responsible for initializing a B-tree index scan. It creates and configures the IndexScanDesc structure along with the associated BTScanOpaque private workspace. This function sets up the foundational structures needed for subsequent scanning operations but does not perform the actual scan initiation - that occurs in btrescan. The function ensures that no order-by operators are used (as B-tree doesn't support them) and prepares memory allocation for scan keys and other scan-related data structures.

## Parameters / Member Variables
- `rel`: The relation (index) to be scanned
- `nkeys`: Number of scan keys that will be used for the scan
- `norderbys`: Number of order-by operators (must be 0 for B-tree indexes)
## Dependencies
- Functions called/Symbols referenced:
  - [RelationGetIndexScan](../R/RelationGetIndexScan.md)
  - BTScanPosInvalidate  
  - [BTScanOpaqueData](../B/BTScanOpaqueData.md)
  - ScanKey
  - [IndexScanDesc](../I/IndexScanDesc.md)
  - BTScanOpaque
- Called from (representative examples):
  - [bthandler](bthandler.md)

## Notes and Other Information
- This function only performs initialization; the actual scan setup with keys occurs in btrescan
- B-tree indexes do not support order-by operators, so norderbys must always be 0
- The function defers allocation of tuple workspace arrays until btrescan to optimize for cases where index-only scans may be possible
- Memory allocation for scan keys is conditional based on the number of keys specified