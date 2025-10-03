# get_matching_hash_bounds

## Location
[src/backend/partitioning/partprune.c:2663-2739](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partprune.c#L2663-L2739)

## Overview
Determines which hash partition bound matches the specified values by computing the hash value and finding the corresponding partition offset.

## Definition

```c
static PruneStepResult *
get_matching_hash_bounds(PartitionPruneContext *context,
						 StrategyNumber opstrategy, Datum *values, int nvalues,
						 FmgrInfo *partsupfunc, Bitmapset *nullkeys)
```
## Detailed Description
This function implements hash partition pruning by calculating the hash value for the given partition key values and determining which specific hash partition should be accessed. For hash partitioning, pruning can only be performed when:

1. All partition keys have either equality clauses or IS NULL clauses
2. The operator strategy is hash equality (HTEqualStrategyNumber)

When all keys are provided, the function computes the partition hash using the supplied values and null indicators, then uses modulo arithmetic to determine the target partition index. If not all keys are provided, it conservatively returns all partition offsets since hash pruning requires complete key information. The function handles the absence of special null or default partitions in hash partitioning.

## Parameters / Member Variables
- `*context`: Partition pruning context containing boundary info and partitioning metadata
- `opstrategy`: Strategy number, must be HTEqualStrategyNumber for hash equality or zero
- `*values`: Array of Datum values indexed by partition key position
- `nvalues`: Number of values in the values array
- `*partsupfunc`: Array of partition hashing functions for each partition key type
- `*nullkeys`: Bitmapset indicating which partition keys are NULL
## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md)
  - [bms_num_members](../b/bms_num_members.md)
  - [bms_is_member](../b/bms_is_member.md)
  - [compute_partition_hash_value](../c/compute_partition_hash_value.md)
  - [bms_make_singleton](../b/bms_make_singleton.md)
  - [bms_add_range](../b/bms_add_range.md)
- Called from:
  - [perform_pruning_base_step](../p/perform_pruning_base_step.md)

## Notes and Other Information
Hash partitioning requires all partition key values to perform effective pruning - partial key information results in scanning all partitions. The function uses the greatest_modulus (total number of partition indexes) to compute the final partition offset. Unlike range and list partitioning, hash partitioning does not support special null or default partitions, so scan_null and scan_default are always set to false. The hash computation considers both explicit values and NULL indicators to ensure consistent partition assignment.

## Simplified Source

```c
static PruneStepResult *
get_matching_hash_bounds(PartitionPruneContext *context,
                         StrategyNumber opstrategy, Datum *values, int nvalues,
                         FmgrInfo *partsupfunc, Bitmapset *nullkeys)
{
    PruneStepResult *result = (PruneStepResult *) palloc0(sizeof(PruneStepResult));
    PartitionBoundInfo boundinfo = context->boundinfo;
    int partnatts = context->partnatts;

    // Hash partitioning can only prune with complete key information
    if (nvalues + bms_num_members(nullkeys) == partnatts) {
        // Build null indicator array
        bool isnull[PARTITION_MAX_KEYS];
        for (int i = 0; i < partnatts; i++)
            isnull[i] = bms_is_member(i, nullkeys);

        // Compute hash value for the complete key
        uint64 rowHash = compute_partition_hash_value(partnatts, partsupfunc,
                                                      context->partcollation,
                                                      values, isnull);

        // Find target partition using modulo arithmetic
        int greatest_modulus = boundinfo->nindexes;
        int target_index = rowHash % greatest_modulus;

        if (boundinfo->indexes[target_index] >= 0)
            result->bound_offsets = bms_make_singleton(target_index);
    } else {
        // Incomplete key information - must scan all partitions
        result->bound_offsets = bms_add_range(NULL, 0, boundinfo->nindexes - 1);
    }

    // Hash partitioning has no special null or default partitions
    result->scan_null = false;
    result->scan_default = false;

    return result;
}
```