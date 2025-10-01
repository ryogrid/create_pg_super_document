# get_matching_list_bounds

## Location
[src/backend/partitioning/partprune.c:2740-2950](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partprune.c#L2740-L2950)

## Overview
Determines which list partition bounds match the specified value according to the given operator strategy (equality, comparison operators).

## Definition

```c
structure to provide us with proofs
	 * that would allow us to do anything smarter here.
	 */
	if (opstrategy != BTEqualStrategyNumber)
		result->scan_default = partition_bound_has_default(boundinfo);
```
## Detailed Description
This function implements list partition pruning by searching through the sorted list of partition bounds to find which partitions should be included based on the specified value and operator strategy. It handles various comparison operators including:

- Equality (=): Finds exact matches using binary search
- Inequality (<>): Returns all partitions except the matching one
- Comparison operators (<, <=, >, >=): Returns ranges of partitions based on bound comparisons

The function properly handles special cases including NULL values (which may go to specific null-accepting partitions or the default partition), empty value sets, and the discontinuous nature of list partitioning where not all values have assigned partitions. For range queries, it conservatively includes the default partition since list partitioning creates gaps in the value space.

## Parameters / Member Variables
- : Partition pruning context containing boundary info and partitioning metadata
- : Btree strategy number indicating the comparison operation to perform
- : The Datum value to use for partition bound matching
- : Number of values provided (should be 1 for list partitioning, or 0 for all values)
- : Partition comparison function for performing binary search on bounds
- : Bitmapset indicating which partition keys are NULL

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md)
  - bms_is_empty
  - partition_bound_accepts_nulls
  - partition_bound_has_default
  - [bms_add_range](../b/bms_add_range.md)
  - [partition_list_bsearch](../p/partition_list_bsearch.md)
  - [bms_del_member](../b/bms_del_member.md)
  - [bms_make_singleton](../b/bms_make_singleton.md)
  - elog
- Called from:
  - [perform_pruning_base_step](../p/perform_pruning_base_step.md)

## Notes and Other Information
List partitioning only supports single-column partition keys, hence the assertion that partnatts == 1. The function sets scan_null and scan_default flags appropriately based on whether special partitions exist and need to be included in the scan. Binary search is used for efficient bound lookup in the sorted partition bounds array. For inequality operations (using InvalidStrategy), the function starts with all partitions and removes the matching one. Range operations conservatively include the default partition due to the potential gaps in list partition value coverage.

## Simplified Source

```c
static PruneStepResult *
get_matching_list_bounds(PartitionPruneContext *context,
                         StrategyNumber opstrategy, Datum value, int nvalues,
                         FmgrInfo *partsupfunc, Bitmapset *nullkeys)
{
    PruneStepResult *result = (PruneStepResult *) palloc0(sizeof(PruneStepResult));
    PartitionBoundInfo boundinfo = context->boundinfo;

    result->scan_null = result->scan_default = false;

    // Handle NULL values
    if (!bms_is_empty(nullkeys)) {
        if (partition_bound_accepts_nulls(boundinfo))
            result->scan_null = true;
        else
            result->scan_default = partition_bound_has_default(boundinfo);
        return result;
    }

    // Handle empty partition bound list
    if (boundinfo->ndatums == 0) {
        result->scan_default = partition_bound_has_default(boundinfo);
        return result;
    }

    // Handle request for all non-null values
    if (nvalues == 0) {
        result->bound_offsets = bms_add_range(NULL, 0, boundinfo->ndatums - 1);
        result->scan_default = partition_bound_has_default(boundinfo);
        return result;
    }

    // Handle different operator strategies
    switch (opstrategy) {
        case BTEqualStrategyNumber: {
            // Find exact match using binary search
            bool is_equal;
            int off = partition_list_bsearch(partsupfunc, context->partcollation,
                                           boundinfo, value, &is_equal);
            if (off >= 0 && is_equal) {
                result->bound_offsets = bms_make_singleton(off);
            } else {
                result->scan_default = partition_bound_has_default(boundinfo);
            }
            break;
        }

        case InvalidStrategy: {
            // Inequality (<>) - all partitions except matching one
            result->bound_offsets = bms_add_range(NULL, 0, boundinfo->ndatums - 1);
            bool is_equal;
            int off = partition_list_bsearch(partsupfunc, context->partcollation,
                                           boundinfo, value, &is_equal);
            if (off >= 0 && is_equal)
                result->bound_offsets = bms_del_member(result->bound_offsets, off);
            result->scan_default = partition_bound_has_default(boundinfo);
            break;
        }

        case BTGreaterStrategyNumber:
        case BTGreaterEqualStrategyNumber:
        case BTLessStrategyNumber:
        case BTLessEqualStrategyNumber: {
            // Range operations - find appropriate bound range
            bool is_equal;
            int off = partition_list_bsearch(partsupfunc, context->partcollation,
                                           boundinfo, value, &is_equal);

            // Calculate minoff/maxoff based on strategy and inclusivity
            int minoff = 0, maxoff = boundinfo->ndatums - 1;
            // ... bound calculation logic ...

            result->bound_offsets = bms_add_range(NULL, minoff, maxoff);
            result->scan_default = partition_bound_has_default(boundinfo);
            break;
        }
    }

    return result;
}
```