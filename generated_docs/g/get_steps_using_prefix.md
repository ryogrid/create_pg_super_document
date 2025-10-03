# get_steps_using_prefix

## Location
[src/backend/partitioning/partprune.c:2438-2495](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partprune.c#L2438-L2495)

## Overview
Generates a list of PartitionPruneStepOps based on given partition key clauses and a final expression to create comprehensive pruning steps.

## Definition

```c
static List *
get_steps_using_prefix(GeneratePruningStepsContext *context,
					   StrategyNumber step_opstrategy,
					   bool step_op_is_ne,
					   Expr *step_lastexpr,
					   Oid step_lastcmpfn,
					   Bitmapset *step_nullkeys,
					   List *prefix)
```
## Detailed Description
This function serves as the entry point for generating partition pruning steps when multiple partition keys are involved. It takes a prefix of PartClauseInfos for earlier partition keys and combines them with a final expression to create all valid combinations of pruning steps. The function handles different partitioning strategies (LIST, RANGE, HASH) with specific requirements for each:

- For LIST/RANGE: step_nullkeys must be NULL and prefix must contain clauses for all prior keys
- For HASH: step_nullkeys can substitute for missing clauses, and prefix must contain clauses for all keys except the final one

When the prefix is empty (single partition key case), it directly generates a single pruning step. Otherwise, it delegates to the recursive helper function to generate all valid combinations.

## Parameters / Member Variables
- `*context`: Context information for generating pruning steps including partition relation and strategy
- `step_opstrategy`: Strategy number for the comparison operation (BTEqualStrategyNumber, etc.)
- `step_op_is_ne`: Boolean indicating if this is a not-equal operation (for LIST partitioning)
- `*step_lastexpr`: Expression for the final partition key in the combination
- `step_lastcmpfn`: Comparison function OID for the final partition key
- `*step_nullkeys`: Bitmapset indicating which keys should be treated as NULL (HASH partitioning only)
- `*prefix`: List of PartClauseInfos for partition keys prior to the final key, sorted by keyno
## Dependencies
- Functions called/Symbols referenced:
  - [gen_prune_step_op](gen_prune_step_op.md)
  - [get_steps_using_prefix_recurse](get_steps_using_prefix_recurse.md)
  - list_make1
  - list_make1_oid
  - [list_head](../l/list_head.md)
- Called from:
  - [gen_prune_steps_from_opexps](gen_prune_steps_from_opexps.md) (multiple locations)

## Notes and Other Information
The function includes important assertions to ensure correct usage based on partitioning strategy. For HASH partitioned tables, step_nullkeys provides a mechanism to handle missing clauses for certain partition keys, which is essential for hash partitioning semantics. The prefix list must be properly sorted by keyno to ensure correct step generation. This function is crucial for multi-column partition key pruning optimization.

## Simplified Source

```c
static List *
get_steps_using_prefix(GeneratePruningStepsContext *context,
                       StrategyNumber step_opstrategy,
                       bool step_op_is_ne,
                       Expr *step_lastexpr,
                       Oid step_lastcmpfn,
                       Bitmapset *step_nullkeys,
                       List *prefix)
{
    // Validate step_nullkeys usage (only for HASH partitioning)
    Assert(step_nullkeys == NULL ||
           context->rel->part_scheme->strategy == PARTITION_STRATEGY_HASH);

    // Handle simple case: single partition key column
    if (prefix == NIL)
    {
        PartitionPruneStep *step;

        step = gen_prune_step_op(context,
                                step_opstrategy,
                                step_op_is_ne,
                                list_make1(step_lastexpr),
                                list_make1_oid(step_lastcmpfn),
                                step_nullkeys);
        return list_make1(step);
    }

    // Multiple partition keys: use recursive helper to generate all combinations
    return get_steps_using_prefix_recurse(context,
                                         step_opstrategy,
                                         step_op_is_ne,
                                         step_lastexpr,
                                         step_lastcmpfn,
                                         step_nullkeys,
                                         prefix,
                                         list_head(prefix),
                                         NIL, NIL);
}
```