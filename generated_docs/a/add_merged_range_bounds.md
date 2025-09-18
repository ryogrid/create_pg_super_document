# add_merged_range_bounds

## Location
src/backend/partitioning/partbounds.c: 2775 - 2851

## Overview
Adds the bounds of a merged partition to the lists of range bounds, handling proper ordering and deduplication of bounds.

## Definition
```c
static void add_merged_range_bounds(int partnatts, FmgrInfo *partsupfuncs,
                                  Oid *partcollations,
                                  PartitionRangeBound *merged_lb,
                                  PartitionRangeBound *merged_ub,
                                  int merged_index,
                                  List **merged_datums,
                                  List **merged_kinds,
                                  List **merged_indexes)
```

## Detailed Description
This function adds the bounds of a newly merged partition to the accumulating lists that track all partition bounds. It implements intelligent bound management by checking if the new lower bound is higher than the previous upper bound - if so, it adds both bounds; otherwise, it reuses the previous upper bound as the lower bound to avoid redundant bounds. The function maintains three parallel lists: datums (boundary values), kinds (boundary types like MINVALUE/MAXVALUE), and indexes (partition identifiers, with -1 indicating lower bounds).

## Parameters / Member Variables
- `partnatts`: Number of partition key attributes
- `partsupfuncs`: Array of comparison functions for partition key types
- `partcollations`: Array of collation OIDs for partition key attributes
- `merged_lb`: Lower bound of the merged partition to add
- `merged_ub`: Upper bound of the merged partition to add
- `merged_index`: Index/identifier of the merged partition
- `merged_datums`: Pointer to list of boundary datum arrays (input/output)
- `merged_kinds`: Pointer to list of boundary kind arrays (input/output)
- `merged_indexes`: Pointer to list of partition indexes (input/output)

## Dependencies
- Functions called/Symbols referenced:
  - [PartitionRangeBound](../P/PartitionRangeBound.md)
  - llast_int
  - llast
  - [PartitionRangeDatumKind](../P/PartitionRangeDatumKind.md)
  - [partition_rbound_cmp](../p/partition_rbound_cmp.md)
  - lappend_int
- Called from (representative examples):
  - [merge_range_bounds](../m/merge_range_bounds.md)

## Notes and Other Information
- This is a static function within partbounds.c used during partition-wise join processing
- The function uses -1 as a special index value to mark lower bounds in the merged_indexes list
- Implements bound deduplication optimization to avoid storing redundant boundary information
- Essential for constructing the final merged partition bounds structure efficiently
- Located in src/backend/partitioning/partbounds.c:2775-2851