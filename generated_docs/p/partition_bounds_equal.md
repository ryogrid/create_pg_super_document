# partition_bounds_equal

## Location
[src/backend/partitioning/partbounds.c:896-1001](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L896-L1001)

## Overview
Determines if two partition bound collections are logically equal by comparing their structural elements and data content.

## Definition

```c
bool
partition_bounds_equal(int partnatts, int16 *parttyplen, bool *parttypbyval,
					   PartitionBoundInfo b1, PartitionBoundInfo b2)
```
## Detailed Description
This function performs a deep comparison of two PartitionBoundInfo structures to determine logical equality. It is used in the relcache keep logic and for comparing partition bounds between different relations. The function compares structural elements first (strategy, counts, indexes) then performs detailed datum-by-datum comparison based on the partitioning strategy.

For hash partitions, it leverages the fact that if indexes arrays match, the bounds are equivalent due to the modulus/remainder organization. For range and list partitions, it performs element-wise comparison of both bound kinds and actual datum values using safe comparison methods.

## Parameters / Member Variables
- `partnatts`: Number of partition attributes
- `*parttyplen`: Array of type lengths for each partition attribute
- `*parttypbyval`: Array indicating if each partition attribute type is passed by value
- `b1`: First PartitionBoundInfo structure to compare
- `b2`: Second PartitionBoundInfo structure to compare
## Dependencies
- Functions called/Symbols referenced:
  - [datumIsEqual](../d/datumIsEqual.md)
- Data types used:
  - [PartitionBoundInfo](../P/PartitionBoundInfo.md)
  - PARTITION_STRATEGY_HASH
  - PARTITION_RANGE_DATUM_VALUE
- Called from:
  - [compute_partition_bounds](../c/compute_partition_bounds.md)
  - partition_bound_has_default

## Notes and Other Information
- Uses datumIsEqual() instead of partitioning operators for safety in aborted transaction contexts
- For hash partitions, relies on indexes array comparison due to modulus/remainder organization
- Handles non-finite bounds (MINVALUE/MAXVALUE) specially for range partitions
- Designed to detect ANY change to partition bounds, not just semantically significant ones
- Critical for relcache invalidation logic to ensure cache consistency

## Simplified Source

```c
bool
partition_bounds_equal(int partnatts, int16 *parttyplen, bool *parttypbyval,
                       PartitionBoundInfo b1, PartitionBoundInfo b2)
{
    int i;

    // Compare basic structural properties
    if (b1->strategy != b2->strategy ||
        b1->ndatums != b2->ndatums ||
        b1->nindexes != b2->nindexes ||
        b1->null_index != b2->null_index ||
        b1->default_index != b2->default_index)
        return false;

    // Compare indexes arrays for all strategies
    for (i = 0; i < b1->nindexes; i++)
    {
        if (b1->indexes[i] != b2->indexes[i])
            return false;
    }

    // Strategy-specific datum comparison
    if (b1->strategy == PARTITION_STRATEGY_HASH)
    {
        // For hash partitions, matching indexes arrays imply equal bounds
        // due to modulus/remainder organization
#ifdef USE_ASSERT_CHECKING
        for (i = 0; i < b1->ndatums; i++)
            Assert((b1->datums[i][0] == b2->datums[i][0] &&
                    b1->datums[i][1] == b2->datums[i][1]));
#endif
    }
    else
    {
        // For range/list partitions, compare datums element by element
        for (i = 0; i < b1->ndatums; i++)
        {
            for (int j = 0; j < partnatts; j++)
            {
                // Check bound kinds for range partitions (finite vs infinite)
                if (b1->kind != NULL)
                {
                    if (b1->kind[i][j] != b2->kind[i][j])
                        return false;

                    // Non-finite bounds (MINVALUE/MAXVALUE) are equal by kind
                    if (b1->kind[i][j] != PARTITION_RANGE_DATUM_VALUE)
                        continue;
                }

                // Compare actual datum values using safe equality
                if (!datumIsEqual(b1->datums[i][j], b2->datums[i][j],
                                  parttypbyval[j], parttyplen[j]))
                    return false;
            }
        }
    }

    return true;
}
```