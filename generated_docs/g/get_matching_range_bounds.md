# get_matching_range_bounds

## Location
[src/backend/partitioning/partprune.c:2951-3345](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partprune.c#L2951-L3345)

## Overview
Determines the offsets of range bounds matching the specified values according to the semantics of the given operator strategy for range partitioned tables.

## Definition

```c
static PruneStepResult *
get_matching_range_bounds(PartitionPruneContext *context,
						  StrategyNumber opstrategy, Datum *values, int nvalues,
						  FmgrInfo *partsupfunc, Bitmapset *nullkeys)
```
## Detailed Description
This function is central to PostgreSQL's partition pruning mechanism for range-partitioned tables. It analyzes partition bounds and operator strategies to determine which partitions might contain data matching the given lookup values. The function handles different B-tree operator strategies (=, <, <=, >, >=) and uses binary search to efficiently locate relevant partition bounds.

The function performs sophisticated logic to handle partial key matches when fewer values are provided than partition key columns, correctly handling inclusive/exclusive bounds, and special cases like MINVALUE and MAXVALUE bounds. It returns a PruneStepResult containing the set of bound offsets that represent partitions potentially containing matching data.

## Parameters / Member Variables
- : PartitionPruneContext containing partition information including bound data, collation, and strategy
- : B-tree strategy number indicating the comparison operator (=, <, <=, >, >=)
- : Array of Datum values to match against partition bounds 
- : Number of values in the values array, must be <= context->partnatts
- : Array of comparison functions for range partitioning operations
- : Bitmapset indicating which partition keys are null

## Dependencies
- Functions called/Symbols referenced:
  - partition_bound_has_default
  - [partition_range_datum_bsearch](../p/partition_range_datum_bsearch.md)
  - [partition_rbound_datum_cmp](../p/partition_rbound_datum_cmp.md)
  - bms_is_empty
  - [bms_add_range](../b/bms_add_range.md)
  - [bms_make_singleton](../b/bms_make_singleton.md)
  - Constants: PARTITION_STRATEGY_RANGE, PARTITION_RANGE_DATUM_MINVALUE, PARTITION_RANGE_DATUM_MAXVALUE
  - Strategy numbers: BTEqualStrategyNumber, BTGreaterStrategyNumber, BTGreaterEqualStrategyNumber, BTLessStrategyNumber, BTLessEqualStrategyNumber
- Called from:
  - [perform_pruning_base_step](../p/perform_pruning_base_step.md)

## Notes and Other Information
- This is a static function used internally within the partition pruning subsystem
- The function implements complex logic for handling edge cases in range partitioning, including partial key matches and infinite bounds
- The scan_default flag in the result indicates whether the default partition needs to be scanned
- The function is optimized for performance using binary search algorithms for bound lookup
- Handles special PostgreSQL partition bound types like MINVALUE and MAXVALUE for representing infinite ranges

## Simplified Source

```c
static PruneStepResult *
get_matching_range_bounds(PartitionPruneContext *context,
                          StrategyNumber opstrategy, Datum *values, int nvalues,
                          FmgrInfo *partsupfunc, Bitmapset *nullkeys)
{
    PruneStepResult *result = (PruneStepResult *) palloc0(sizeof(PruneStepResult));
    PartitionBoundInfo boundinfo = context->boundinfo;

    result->scan_null = result->scan_default = false;

    // Handle empty bounds or NULL keys - use default partition
    if (boundinfo->ndatums == 0 || !bms_is_empty(nullkeys)) {
        result->scan_default = partition_bound_has_default(boundinfo);
        return result;
    }

    // Handle incomplete key specification
    if (nvalues < context->partnatts)
        result->scan_default = partition_bound_has_default(boundinfo);

    // Handle request for all non-null values
    if (nvalues == 0) {
        int minoff = 0, maxoff = boundinfo->ndatums;
        // Skip invalid partitions at boundaries
        if (boundinfo->indexes[minoff] < 0) minoff++;
        if (boundinfo->indexes[maxoff] < 0) maxoff--;

        result->bound_offsets = bms_add_range(NULL, minoff, maxoff);
        result->scan_default = partition_bound_has_default(boundinfo);
        return result;
    }

    // Handle different operator strategies
    switch (opstrategy) {
        case BTEqualStrategyNumber: {
            // Find exact match using binary search
            bool is_equal;
            int off = partition_range_datum_bsearch(partsupfunc, context->partcollation,
                                                   boundinfo, nvalues, values, &is_equal);

            if (off >= 0 && is_equal) {
                if (nvalues == context->partnatts) {
                    // Complete key - single partition
                    result->bound_offsets = bms_make_singleton(off + 1);
                } else {
                    // Partial key - find range of matching bounds
                    int minoff = off, maxoff = off + 1;
                    // Expand range to include all matching prefix bounds
                    result->bound_offsets = bms_add_range(NULL, minoff, maxoff);
                }
            } else {
                // Value falls between bounds
                result->bound_offsets = bms_make_singleton(off + 1);
            }
            break;
        }

        case BTGreaterStrategyNumber:
        case BTGreaterEqualStrategyNumber: {
            // Find minimum bound for greater-than queries
            bool is_equal;
            int off = partition_range_datum_bsearch(partsupfunc, context->partcollation,
                                                   boundinfo, nvalues, values, &is_equal);

            int minoff = (off < 0) ? 0 : off + 1;
            // Handle inclusive/exclusive and partial keys
            if (is_equal && opstrategy == BTGreaterEqualStrategyNumber)
                minoff = off;

            result->bound_offsets = bms_add_range(NULL, minoff, boundinfo->ndatums);
            break;
        }

        case BTLessStrategyNumber:
        case BTLessEqualStrategyNumber: {
            // Find maximum bound for less-than queries
            bool is_equal;
            int off = partition_range_datum_bsearch(partsupfunc, context->partcollation,
                                                   boundinfo, nvalues, values, &is_equal);

            int maxoff = (off < 0) ? 0 : off + 1;
            // Handle inclusive/exclusive
            if (is_equal && opstrategy == BTLessStrategyNumber)
                maxoff = off;

            result->bound_offsets = bms_add_range(NULL, 0, maxoff);
            break;
        }
    }

    // Exclude invalid partitions (MINVALUE/MAXVALUE edge cases)
    // ... boundary adjustment logic ...

    return result;
}
```