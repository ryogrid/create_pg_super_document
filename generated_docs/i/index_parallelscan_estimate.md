# index_parallelscan_estimate

## Location
[src/backend/access/index/indexam.c:453-489](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/index/indexam.c#L453-L489)

## Overview
The index_parallelscan_estimate function calculates the amount of shared memory required for a parallel index scan operation, including space for the scan descriptor, snapshot data, and access method-specific data.

## Definition
```c
Size index_parallelscan_estimate(Relation indexRelation, int nkeys, int norderbys, Snapshot snapshot)
```

## Detailed Description
index_parallelscan_estimate computes the shared memory requirements for setting up a parallel index scan. It calculates space needed for the base ParallelIndexScanDescData structure, the snapshot data, and any additional space required by the specific index access method. The function ensures proper memory alignment and handles cases where access methods may not provide parallel scan estimation routines.

The calculation includes the base parallel scan descriptor, estimated snapshot space, and optional AM-specific data. The result is properly aligned and can be used by the parallel query infrastructure to allocate sufficient shared memory for the parallel scan operation.

## Parameters / Member Variables
- `indexRelation`: Relation - The index relation that will be scanned in parallel
- `nkeys`: int - Number of scan keys that will be used in the parallel scan
- `norderbys`: int - Number of ORDER BY expressions for the scan
- `snapshot`: Snapshot - The snapshot to be used for the parallel scan

## Dependencies
- Functions called/Symbols referenced:
  - RELATION_CHECKS (validation macro for relation)
  - [ParallelIndexScanDescData](../P/ParallelIndexScanDescData.md) (parallel scan descriptor structure)
  - [EstimateSnapshotSpace](../E/EstimateSnapshotSpace.md) (estimates memory needed for snapshot data)
  - [add_size](../a/add_size.md) (safe size addition utility)
  - MAXALIGN (memory alignment macro)
  - amestimateparallelscan (access method-specific estimation routine)
- Called from (representative examples):
  - [ExecIndexScanEstimate](../E/ExecIndexScanEstimate.md)
  - [ExecIndexOnlyScanEstimate](../E/ExecIndexOnlyScanEstimate.md)

## Notes and Other Information
- Essential for parallel query processing - provides memory requirements before scan initialization
- Handles access methods that don't provide amestimateparallelscan by assuming no AM-specific data needed
- Includes validation to ensure snapshot is not InvalidSnapshot
- Memory calculation includes proper alignment considerations for shared memory structures
- The estimated size is used by the parallel query coordinator to allocate shared memory segments
- Located in src/backend/access/index/indexam.c:453-489
- Part of PostgreSQL's parallel query execution framework introduced for performance improvements

## Simplified Source

```c
Size index_parallelscan_estimate(Relation indexRelation, int nkeys, int norderbys, Snapshot snapshot) {
    Size nbytes;

    // Validate inputs
    Assert(snapshot != InvalidSnapshot);
    RELATION_CHECKS;

    // Calculate base size: descriptor + snapshot data
    nbytes = offsetof(ParallelIndexScanDescData, ps_snapshot_data);
    nbytes = add_size(nbytes, EstimateSnapshotSpace(snapshot));
    nbytes = MAXALIGN(nbytes);

    // Add AM-specific parallel scan data if supported
    if (indexRelation->rd_indam->amestimateparallelscan != NULL) {
        nbytes = add_size(nbytes,
                         indexRelation->rd_indam->amestimateparallelscan(nkeys, norderbys));
    }

    return nbytes;
}
```