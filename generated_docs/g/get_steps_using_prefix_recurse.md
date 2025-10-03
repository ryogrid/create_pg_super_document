# get_steps_using_prefix_recurse

## Location
[src/backend/partitioning/partprune.c:2496-2662](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partprune.c#L2496-L2662)

## Overview
Recursively generates all valid combinations of PartitionPruneStepOps when multiple PartClauseInfos exist for the same partition key.

## Definition

```c
static List *
get_steps_using_prefix_recurse(GeneratePruningStepsContext *context,
							   StrategyNumber step_opstrategy,
							   bool step_op_is_ne,
							   Expr *step_lastexpr,
							   Oid step_lastcmpfn,
							   Bitmapset *step_nullkeys,
							   List *prefix,
							   ListCell *start,
							   List *step_exprs,
							   List *step_cmpfns)
```
## Detailed Description
This function implements the recursive core of multi-key partition pruning step generation. It handles the complex case where multiple PartClauseInfos exist for the same partition key, creating a cartesian product of all valid combinations. The recursion proceeds by:

1. Finding all PartClauseInfos for the current partition key
2. For each clause, recursively processing remaining keys with updated expression/function lists
3. At the final recursion level, generating actual pruning steps by combining all accumulated expressions

The function maintains careful bookkeeping of expressions and comparison functions for each partition key, ensuring that the final pruning steps contain exactly one expression per partition key. Special handling exists for hash partitioning where NULL keys are allowed via the step_nullkeys bitmapset.

## Parameters / Member Variables
- `*context`: Context information for generating pruning steps
- `step_opstrategy`: Strategy number for the comparison operation
- `step_op_is_ne`: Boolean indicating if this is a not-equal operation
- `*step_lastexpr`: Expression for the final partition key
- `step_lastcmpfn`: Comparison function OID for the final partition key
- `*step_nullkeys`: Bitmapset indicating which keys should be treated as NULL
- `*prefix`: List of PartClauseInfos sorted by keyno
- `*start`: Starting point in the prefix list for this recursion level
- `*step_exprs`: Accumulated expressions from previous partition keys
- `*step_cmpfns`: Accumulated comparison functions from previous partition keys
## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md)
  - lfirst
  - llast
  - for_each_cell
  - [list_copy](../l/list_copy.md)
  - [lappend](../l/lappend.md)
  - [lappend_oid](../l/lappend_oid.md)
  - [list_concat](../l/list_concat.md)
  - [list_free](../l/list_free.md)
  - [gen_prune_step_op](gen_prune_step_op.md)
  - bms_is_empty
  - [bms_num_members](../b/bms_num_members.md)
- Called from:
  - [get_steps_using_prefix](get_steps_using_prefix.md)
  - [get_steps_using_prefix_recurse](get_steps_using_prefix_recurse.md) (recursive)

## Notes and Other Information
The function includes important assertions to validate the structure for hash partitioning, ensuring that each partition key has either an equality clause or is marked as NULL in step_nullkeys. The recursion is bounded by PARTITION_MAX_KEYS to prevent stack overflow. Memory management is carefully handled by copying and freeing intermediate expression/function lists to avoid modifying shared data structures. This function is critical for optimizing queries with complex multi-column partition key predicates involving multiple clauses per key.

## Simplified Source

```c
static List *
get_steps_using_prefix_recurse(GeneratePruningStepsContext *context,
                               StrategyNumber step_opstrategy,
                               bool step_op_is_ne,
                               Expr *step_lastexpr,
                               Oid step_lastcmpfn,
                               Bitmapset *step_nullkeys,
                               List *prefix,
                               ListCell *start,
                               List *step_exprs,
                               List *step_cmpfns)
{
    List *result = NIL;
    ListCell *lc;
    int cur_keyno;
    int final_keyno;

    // Stack depth check to prevent overflow
    check_stack_depth();

    cur_keyno = ((PartClauseInfo *) lfirst(start))->keyno;
    final_keyno = ((PartClauseInfo *) llast(prefix))->keyno;

    // Check if we need to recurse to next partition key
    if (cur_keyno < final_keyno)
    {
        PartClauseInfo *pc;
        ListCell *next_start;

        // Find start of next partition key
        for_each_cell(lc, prefix, start)
        {
            pc = lfirst(lc);
            if (pc->keyno > cur_keyno)
                break;
        }
        next_start = lc;

        // Process each clause for current key
        for_each_cell(lc, prefix, start)
        {
            pc = lfirst(lc);
            if (pc->keyno == cur_keyno)
            {
                // Build expression and function lists for this combination
                List *step_exprs1 = list_copy(step_exprs);
                List *step_cmpfns1 = list_copy(step_cmpfns);

                step_exprs1 = lappend(step_exprs1, pc->expr);
                step_cmpfns1 = lappend_oid(step_cmpfns1, pc->cmpfn);

                // Recurse for remaining keys
                List *moresteps = get_steps_using_prefix_recurse(context,
                                                               step_opstrategy, step_op_is_ne,
                                                               step_lastexpr, step_lastcmpfn,
                                                               step_nullkeys, prefix, next_start,
                                                               step_exprs1, step_cmpfns1);
                result = list_concat(result, moresteps);

                // Clean up temporary lists
                list_free(step_exprs1);
                list_free(step_cmpfns1);
            }
            else
                break;
        }
    }
    else
    {
        // Base case: generate pruning steps for final key
        for_each_cell(lc, prefix, start)
        {
            PartClauseInfo *pc = lfirst(lc);
            PartitionPruneStep *step;

            // Build final expression and function lists
            List *step_exprs1 = list_copy(step_exprs);
            List *step_cmpfns1 = list_copy(step_cmpfns);

            step_exprs1 = lappend(step_exprs1, pc->expr);
            step_exprs1 = lappend(step_exprs1, step_lastexpr);
            step_cmpfns1 = lappend_oid(step_cmpfns1, pc->cmpfn);
            step_cmpfns1 = lappend_oid(step_cmpfns1, step_lastcmpfn);

            // Generate the actual pruning step
            step = gen_prune_step_op(context, step_opstrategy, step_op_is_ne,
                                   step_exprs1, step_cmpfns1, step_nullkeys);
            result = lappend(result, step);
        }
    }

    return result;
}
```