# add_merged_range_bounds

## Location
[src/backend/partitioning/partbounds.c:2775-2851](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L2775-L2851)

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
  - [lappend_int](../l/lappend_int.md)
- Called from (representative examples):
  - [merge_range_bounds](../m/merge_range_bounds.md)

## Notes and Other Information
- This is a static function within partbounds.c used during partition-wise join processing
- The function uses -1 as a special index value to mark lower bounds in the merged_indexes list
- Implements bound deduplication optimization to avoid storing redundant boundary information
- Essential for constructing the final merged partition bounds structure efficiently
- Located in src/backend/partitioning/partbounds.c:2775-2851

## Simplified Source

```c
static void
add_merged_range_bounds(int partnatts, FmgrInfo *partsupfuncs,
                       Oid *partcollations,
                       PartitionRangeBound *merged_lb,
                       PartitionRangeBound *merged_ub,
                       int merged_index,
                       List **merged_datums,
                       List **merged_kinds,
                       List **merged_indexes)
{
    int cmpval;

    if (!*merged_datums)
    {
        // First merged partition - no comparison needed
        cmpval = 1;
    }
    else
    {
        // Compare new lower bound with previous upper bound
        PartitionRangeBound prev_ub;
        prev_ub.index = llast_int(*merged_indexes);
        prev_ub.datums = (Datum *) llast(*merged_datums);
        prev_ub.kind = (PartitionRangeDatumKind *) llast(*merged_kinds);
        prev_ub.lower = false;

        cmpval = partition_rbound_cmp(partnatts, partsupfuncs, partcollations,
                                     merged_lb->datums, merged_lb->kind,
                                     false, &prev_ub);
    }

    // Add lower bound only if it's higher than previous upper bound
    if (cmpval > 0)
    {
        *merged_datums = lappend(*merged_datums, merged_lb->datums);
        *merged_kinds = lappend(*merged_kinds, merged_lb->kind);
        *merged_indexes = lappend_int(*merged_indexes, -1);  // -1 marks lower bound
    }

    // Always add the upper bound
    *merged_datums = lappend(*merged_datums, merged_ub->datums);
    *merged_kinds = lappend(*merged_kinds, merged_ub->kind);
    *merged_indexes = lappend_int(*merged_indexes, merged_index);
}
```