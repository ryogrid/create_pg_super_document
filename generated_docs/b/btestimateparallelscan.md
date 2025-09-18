# btestimateparallelscan

## Location
src/backend/access/nbtree/nbtree.c: 537 - 546

## Overview
Estimates the storage size required for BTParallelScanDescData structure to support parallel btree index scans.

## Definition
```c
Size btestimateparallelscan(int nkeys, int norderbys)
```

## Detailed Description
This function calculates the memory size needed to allocate a BTParallelScanDescData structure for coordinating parallel btree scans. The function makes a pessimistic assumption that all input scan keys will be output with arrays, which ensures sufficient memory allocation for the parallel scan coordination data structure.

The calculation includes the base size of BTParallelScanDescData plus additional space for array elements based on the number of keys.

## Parameters / Member Variables
- `nkeys`: Number of scan keys that will be used in the parallel scan
- `norderbys`: Number of order-by expressions (currently not used in the calculation)

## Dependencies
- Functions called/Symbols referenced:
  - BTParallelScanDescData (structure type)
  - offsetof (macro)
  - sizeof (operator)
- Called from (representative examples):
  - bthandler

## Notes and Other Information
- The function takes a pessimistic approach by assuming all scan keys will require array storage
- The `norderbys` parameter is accepted but not currently used in the size calculation
- This is part of PostgreSQL's parallel query infrastructure for btree indexes
- The returned size is used by the parallel scan coordinator to allocate shared memory