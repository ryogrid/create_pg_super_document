# PartitionRangeBound

## Location
[src/backend/partitioning/partbounds.c:64-70](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L64-L70)

## Overview
PartitionRangeBound represents one bound (either lower or upper) of a range partition, used in PostgreSQL's range partitioning implementation.

## Definition
```c
typedef struct PartitionRangeBound
{
    int         index;
    Datum      *datums;             /* range bound datums */
    PartitionRangeDatumKind *kind;  /* the kind of each datum */
    bool        lower;              /* this is the lower (vs upper) bound */
} PartitionRangeBound;
```

## Detailed Description
PartitionRangeBound is a structure that represents a single boundary in range partitioning. Range partitioning divides data based on ranges of values, where each partition is defined by lower and upper bounds. This structure encapsulates all the information needed to define one such bound, including the actual values, their types, and whether this represents a lower or upper boundary. It supports multi-column partition keys by storing arrays of datums and their corresponding kinds.

## Parameters / Member Variables
- `index`: The index or identifier of the partition that this bound belongs to
- `datums`: Array of Datum values that constitute this range bound (supports multi-column partitioning)
- `kind`: Array of PartitionRangeDatumKind values indicating the type of each datum (e.g., finite value, infinity, etc.)
- `lower`: Boolean flag indicating whether this is a lower bound (true) or upper bound (false)

## Dependencies
- Functions called/Symbols referenced:
  - [PartitionRangeDatumKind](PartitionRangeDatumKind.md) (enum for datum types)
  - Datum (PostgreSQL data type)
- Called from (representative examples):
  - compare_range_bounds (multiple references)
  - [create_range_bounds](../c/create_range_bounds.md) (multiple references)
  - [merge_range_bounds](../m/merge_range_bounds.md)
  - [get_range_partition](../g/get_range_partition.md)
  - [partition_rbound_cmp](../p/partition_rbound_cmp.md)
  - [qsort_partition_rbound_cmp](../q/qsort_partition_rbound_cmp.md)

## Notes and Other Information
This structure is central to PostgreSQL's range partitioning implementation and is used extensively in bound comparison, creation, merging, and lookup operations. The arrays of datums and kinds allow for complex multi-column range partitioning. The structure is designed to work with PostgreSQL's sorting and binary search algorithms for efficient partition lookup.