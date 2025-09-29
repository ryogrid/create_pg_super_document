# partition_bounds_copy

## Location
[src/backend/partitioning/partbounds.c:1002-1117](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L1002-L1117)

## Overview
Creates a deep copy of a PartitionBoundInfo structure, duplicating all data elements while respecting memory management constraints for long-lived contexts.

## Definition

```c
PartitionBoundInfo
partition_bounds_copy(PartitionBoundInfo src,
					  PartitionKey key)
```
## Detailed Description
This function creates a complete deep copy of a PartitionBoundInfo structure, carefully copying all data elements including datums, indexes, and metadata. It handles different partitioning strategies (hash, range, list) appropriately, using the partition key specification to determine data types and copying behavior. The function is designed to avoid catalog access and unwanted memory leaks in long-lived contexts.

Key copying behaviors:
- Allocates new memory for all structure elements
- Deep copies datum values using appropriate type-specific methods
- Handles range partition kinds (MINVALUE/MAXVALUE) specially
- For hash partitions, treats datums as int32 modulus/remainder pairs
- Copies interleaved partition bitmaps for list partitions

## Parameters / Member Variables
- : Source PartitionBoundInfo structure to copy from
- : PartitionKey containing partitioning metadata (data types, strategy, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [datumCopy](../d/datumCopy.md)
  - [bms_copy](../b/bms_copy.md)
  - [palloc](palloc.md)
  - memcpy
- Data types used:
  - [PartitionBoundInfo](../P/PartitionBoundInfo.md)
  - [PartitionBoundInfoData](../P/PartitionBoundInfoData.md)
  - [PartitionKey](../P/PartitionKey.md)
  - [PartitionRangeDatumKind](../P/PartitionRangeDatumKind.md)
- Constants used:
  - PARTITION_STRATEGY_HASH
  - PARTITION_STRATEGY_RANGE
  - PARTITION_STRATEGY_LIST
  - PARTITION_RANGE_DATUM_VALUE
- Called from:
  - [RelationBuildPartitionDesc](../R/RelationBuildPartitionDesc.md)
  - partition_bound_has_default

## Notes and Other Information
- Designed for long-lived memory contexts - avoids catalog access and memory leaks
- Optimizes memory allocation by using single large arrays instead of many small ones
- [List](../L/List.md) partitions are constrained to single partition key (partnatts == 1)
- [Hash](../H/Hash.md) partitions always use int32 for modulus/remainder values
- Only copies actual datum values for PARTITION_RANGE_DATUM_VALUE kinds
- Critical for relation descriptor building and caching infrastructure

## Simplified Source

```c
PartitionBoundInfo
partition_bounds_copy(PartitionBoundInfo src, PartitionKey key)
{
    PartitionBoundInfo dest;
    int i, ndatums, nindexes, partnatts;
    bool hash_part;
    int natts;
    Datum *boundDatums;

    // Allocate destination structure
    dest = (PartitionBoundInfo) palloc(sizeof(PartitionBoundInfoData));

    // Copy basic properties
    dest->strategy = src->strategy;
    ndatums = dest->ndatums = src->ndatums;
    nindexes = dest->nindexes = src->nindexes;
    partnatts = key->partnatts;

    // Allocate arrays
    dest->datums = (Datum **) palloc(sizeof(Datum *) * ndatums);

    // Copy range partition kinds for RANGE partitioning
    if (src->kind != NULL)
    {
        PartitionRangeDatumKind *boundKinds;

        dest->kind = (PartitionRangeDatumKind **) palloc(ndatums *
                                    sizeof(PartitionRangeDatumKind *));

        // Allocate single chunk for efficiency
        boundKinds = (PartitionRangeDatumKind *) palloc(ndatums * partnatts *
                                    sizeof(PartitionRangeDatumKind));

        for (i = 0; i < ndatums; i++)
        {
            dest->kind[i] = &boundKinds[i * partnatts];
            memcpy(dest->kind[i], src->kind[i],
                   sizeof(PartitionRangeDatumKind) * partnatts);
        }
    }
    else
        dest->kind = NULL;

    // Copy interleaved partitions bitmap for LIST partitions
    dest->interleaved_parts = bms_copy(src->interleaved_parts);

    // Copy datums with appropriate handling for each partition strategy
    hash_part = (key->strategy == PARTITION_STRATEGY_HASH);
    natts = hash_part ? 2 : partnatts; // Hash uses modulus/remainder pairs
    boundDatums = palloc(ndatums * natts * sizeof(Datum));

    for (i = 0; i < ndatums; i++)
    {
        dest->datums[i] = &boundDatums[i * natts];

        for (int j = 0; j < natts; j++)
        {
            bool byval;
            int typlen;

            if (hash_part)
            {
                // Hash partitions always use int32
                typlen = sizeof(int32);
                byval = true;
            }
            else
            {
                // Use partition key type info
                byval = key->parttypbyval[j];
                typlen = key->parttyplen[j];
            }

            // Only copy actual values, not MINVALUE/MAXVALUE markers
            if (dest->kind == NULL ||
                dest->kind[i][j] == PARTITION_RANGE_DATUM_VALUE)
            {
                dest->datums[i][j] = datumCopy(src->datums[i][j],
                                             byval, typlen);
            }
        }
    }

    // Copy indexes array
    dest->indexes = (int *) palloc(sizeof(int) * nindexes);
    memcpy(dest->indexes, src->indexes, sizeof(int) * nindexes);

    // Copy special indexes
    dest->null_index = src->null_index;
    dest->default_index = src->default_index;

    return dest;
}
```