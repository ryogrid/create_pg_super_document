# perform_pruning_base_step

## Location
[src/backend/partitioning/partprune.c:3416-3563](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partprune.c#L3416-L3563)

## Overview
Determines the indexes of datums that satisfy conditions specified in a partition pruning step, including whether special null-accepting and/or default partitions need to be scanned.

## Definition

```c
static PruneStepResult *
perform_pruning_base_step(PartitionPruneContext *context,
						  PartitionPruneStepOp *opstep)
```
## Detailed Description
This function is the core execution engine for individual partition pruning steps in PostgreSQL's partition pruning system. It takes a pruning step operation and evaluates it against partition bounds to determine which partitions might contain matching data.

The function first builds a partition lookup key by extracting values from the step's expressions, handling null values appropriately, and setting up comparison functions. It then delegates to the appropriate strategy-specific function (hash, list, or range partitioning) to perform the actual bound matching. For range partitioning, it enforces the requirement that values must be provided for either all partition keys or a prefix thereof.

The function handles cross-type comparisons by setting up appropriate comparison functions and manages function caching for performance. It returns a PruneStepResult indicating which partition bounds match the pruning criteria.

## Parameters / Member Variables
- `*context`: PartitionPruneContext containing partition metadata, bounds, and cached comparison functions
- `*opstep`: PartitionPruneStepOp containing the pruning operation details including expressions, comparison functions, operator strategy, and null keys
## Dependencies
- Functions called/Symbols referenced:
  - [list_length](../l/list_length.md), list_head, lnext (list operations)
  - [bms_is_member](../b/bms_is_member.md) (bitmap set operations)  
  - [partkey_datum_from_expr](partkey_datum_from_expr.md)
  - [fmgr_info_copy](../f/fmgr_info_copy.md), fmgr_info_cxt (function manager operations)
  - [get_matching_hash_bounds](../g/get_matching_hash_bounds.md)
  - [get_matching_list_bounds](../g/get_matching_list_bounds.md)
  - [get_matching_range_bounds](../g/get_matching_range_bounds.md)
  - PruneCxtStateIdx (macro)
  - Constants: PARTITION_MAX_KEYS, PARTITION_STRATEGY_HASH, PARTITION_STRATEGY_LIST, PARTITION_STRATEGY_RANGE
- Called from:
  - [get_matching_partitions](../g/get_matching_partitions.md)

## Notes and Other Information
- This is a static function that serves as the main entry point for executing partition pruning steps
- The function enforces strict operator semantics - null values in comparisons cause no partitions to match
- Function caching is used to optimize repeated calls with the same comparison functions
- For range partitioning, the function respects the constraint that values must form a prefix of the partition key
- The function handles all three PostgreSQL partitioning strategies (hash, list, range) through delegation
- Part of PostgreSQL's constraint exclusion and partition-wise optimization infrastructure

## Simplified Source

```c
static PruneStepResult *perform_pruning_base_step(PartitionPruneContext *context,
                                                  PartitionPruneStepOp *opstep) {
    Datum values[PARTITION_MAX_KEYS];
    int keyno, nvalues = 0;
    ListCell *lc1 = list_head(opstep->exprs);
    ListCell *lc2 = list_head(opstep->cmpfns);

    // Build partition lookup key from expressions
    for (keyno = 0; keyno < context->partnatts; keyno++) {
        // Skip null keys for hash partitioning
        if (bms_is_member(keyno, opstep->nullkeys))
            continue;

        // Range partitioning requires prefix of keys
        if (keyno > nvalues && context->strategy == PARTITION_STRATEGY_RANGE)
            break;

        if (lc1 != NULL) {
            Expr *expr = lfirst(lc1);
            Datum datum;
            bool isnull;

            // Extract datum value from expression
            partkey_datum_from_expr(context, expr,
                                  PruneCxtStateIdx(context->partnatts, opstep->step.step_id, keyno),
                                  &datum, &isnull);

            // Null values cause no partitions to match (strict operators)
            if (isnull) {
                PruneStepResult *result = palloc(sizeof(PruneStepResult));
                result->bound_offsets = NULL;
                result->scan_default = false;
                result->scan_null = false;
                return result;
            }

            // Set up comparison function if needed
            Oid cmpfn = lfirst_oid(lc2);
            int stateidx = PruneCxtStateIdx(context->partnatts, opstep->step.step_id, keyno);
            if (cmpfn != context->stepcmpfuncs[stateidx].fn_oid) {
                if (cmpfn == context->partsupfunc[keyno].fn_oid)
                    fmgr_info_copy(&context->stepcmpfuncs[stateidx],
                                 &context->partsupfunc[keyno], context->ppccontext);
                else
                    fmgr_info_cxt(cmpfn, &context->stepcmpfuncs[stateidx], context->ppccontext);
            }

            values[keyno] = datum;
            nvalues++;
            lc1 = lnext(opstep->exprs, lc1);
            lc2 = lnext(opstep->cmpfns, lc2);
        }
    }

    // Delegate to strategy-specific function
    FmgrInfo *partsupfunc = &context->stepcmpfuncs[PruneCxtStateIdx(context->partnatts, opstep->step.step_id, 0)];

    switch (context->strategy) {
        case PARTITION_STRATEGY_HASH:
            return get_matching_hash_bounds(context, opstep->opstrategy, values, nvalues, partsupfunc, opstep->nullkeys);
        case PARTITION_STRATEGY_LIST:
            return get_matching_list_bounds(context, opstep->opstrategy, values[0], nvalues, &partsupfunc[0], opstep->nullkeys);
        case PARTITION_STRATEGY_RANGE:
            return get_matching_range_bounds(context, opstep->opstrategy, values, nvalues, partsupfunc, opstep->nullkeys);
        default:
            elog(ERROR, "unexpected partition strategy: %d", (int) context->strategy);
    }

    return NULL;
}
```