# BrinOpaque

## Location
[src/backend/access/brin/brin.c:199-204](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin.c#L199-L204)

## Overview
BrinOpaque is a structure used as opaque data during BRIN index scans, storing essential access structures and metadata needed throughout the scan operation.

## Definition

```c
typedef struct BrinOpaque
{
	BlockNumber bo_pagesPerRange;
	BrinRevmap *bo_rmAccess;
	BrinDesc   *bo_bdesc;
} BrinOpaque;
```
## Detailed Description
BrinOpaque serves as the opaque data structure passed between scan initialization, execution, and cleanup phases during BRIN index scans. It encapsulates the essential components needed for efficient index scanning: the reverse map for range-to-block mapping, the index descriptor for operator class information, and the pages per range configuration.

The structure follows PostgreSQL's index access method convention where scan-specific state is stored in an opaque structure that gets attached to the IndexScanDesc and passed between scan operations.

## Parameters / Member Variables
- : Number of heap pages covered by each BRIN range in this index
- : Pointer to the BRIN reverse map access structure used for mapping ranges to heap blocks
- : Pointer to the BRIN index descriptor containing operator class definitions and index metadata

## Dependencies
- Functions called/Symbols referenced:
  - [BrinRevmap](BrinRevmap.md)
  - [BrinDesc](BrinDesc.md)
- Called from (representative examples):
  - [brinbeginscan](../b/brinbeginscan.md)
  - [bringetbitmap](../b/bringetbitmap.md)
  - [brinendscan](../b/brinendscan.md)

## Notes and Other Information
This structure is allocated during brinbeginscan and freed during brinendscan, maintaining scan state throughout the index scan lifecycle. The opaque structure pattern allows the generic index scan infrastructure to work with BRIN-specific data without knowing the internal details. All three fields are essential for proper BRIN scan operation and are initialized once at scan start to avoid repeated setup overhead.