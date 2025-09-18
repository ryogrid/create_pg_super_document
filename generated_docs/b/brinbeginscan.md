# brinbeginscan

## Location
src/backend/access/brin/brin.c: 530 - 557

## Overview
The `brinbeginscan` function initializes the state and structures necessary for performing a scan on a BRIN (Block Range Index), setting up the scan descriptor with BRIN-specific opaque data.

## Definition
```c
IndexScanDesc brinbeginscan(Relation r, int nkeys, int norderbys)
```

## Detailed Description
This function is called at the beginning of a BRIN index scan to initialize all the necessary structures and state information. It performs several key setup operations:

1. Creates a generic `IndexScanDesc` structure using `RelationGetIndexScan()`
2. Allocates and initializes a `BrinOpaque` structure to hold BRIN-specific scan state
3. Initializes the revmap (reverse mapping) access structure via `brinRevmapInitialize()`
4. Creates the BRIN descriptor containing index metadata via `brin_build_desc()`
5. Associates the opaque data with the scan descriptor

The function reads the index metapage to determine the pages-per-range parameter that was used when the index was built. This value is critical for determining which BRIN tuples correspond to which ranges of heap pages during the scan. Since the pages-per-range value cannot change while the index is locked, it only needs to be read once during scan initialization.

## Parameters / Member Variables
- `r`: The BRIN index relation to be scanned
- `nkeys`: Number of scan keys (search conditions) for the scan
- `norderbys`: Number of order-by expressions (typically 0 for BRIN indexes)

The created `BrinOpaque` structure contains:
- `bo_rmAccess`: Revmap access structure for mapping page ranges to index tuples
- `bo_pagesPerRange`: Number of heap pages covered by each BRIN index range
- `bo_bdesc`: BRIN descriptor containing index column metadata and operator information

## Dependencies
- Functions called/Symbols referenced:
  - [RelationGetIndexScan](../R/RelationGetIndexScan.md)() (creates generic index scan descriptor)
  - `palloc_object()` (allocates BrinOpaque structure)
  - [brinRevmapInitialize](brinRevmapInitialize.md)() (initializes revmap access)
  - [brin_build_desc](brin_build_desc.md)() (creates BRIN descriptor)
  - [BrinOpaque](../B/BrinOpaque.md) (structure type)
  - [IndexScanDesc](../I/IndexScanDesc.md) (structure type)

- Called from (representative examples):
  - PostgreSQL index access method infrastructure (via `brinhandler()`)
  - [Query](../Q/Query.md) execution when a BRIN index is selected for scanning

## Notes and Other Information
- This function is part of the standard PostgreSQL index access method interface
- The pages-per-range value is read from the index metapage and cached for the duration of the scan
- BRIN indexes typically do not support ordering, so `norderbys` is usually 0
- The opaque data structure lifetime is managed by the scan descriptor and will be cleaned up when the scan ends
- The revmap structure is essential for BRIN scans as it provides the mapping between heap page ranges and their corresponding index tuples
- Unlike B-tree indexes, BRIN scans work by examining summary information for page ranges rather than individual tuples