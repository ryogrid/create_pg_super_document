# gen_partprune_steps_internal

## Location
[src/backend/partitioning/partprune.c:961-1312](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partprune.c#L961-L1312)

## Overview
Processes a list of WHERE clauses to generate partition pruning steps that can be used to eliminate partitions during query execution, returning NIL when no pruning steps can be generated.

## Definition

```c
struct a dummy PartitionPruneStepCombine whose
						 * source_stepids is set to an empty List.
						 */
						orstep = gen_prune_step_combine(context, NIL,
														PARTPRUNE_COMBINE_UNION);
```
## Detailed Description
This is the core function for generating partition pruning steps from SQL WHERE clauses. It recursively processes various clause types (BoolExpr, OpExpr, IS NULL/NOT NULL) and creates two types of pruning steps:

1. **Operator Steps (PartitionPruneStepOp)**: Contain details of expressions being compared to partition keys with comparison functions
2. **Combine Steps (PartitionPruneStepCombine)**: Instructions for merging results from multiple steps using UNION (OR logic) or INTERSECT (AND logic)

The function handles three main pruning strategies:
- **Strategy 1**: IS NULL clauses - generates steps to select only null-accepting partitions
- **Strategy 2**: OpExpr-based pruning using partition key comparisons
- **Strategy 3**: IS NOT NULL clauses - generates steps to exclude null-accepting partitions

For BoolExpr clauses, it processes arguments recursively: OR expressions use UNION combine steps, AND expressions use INTERSECT combine steps. The function detects contradictory clauses and optimization opportunities, setting context->contradictory when pruning can eliminate all partitions.

## Parameters / Member Variables
- : GeneratePruningStepsContext containing partition metadata, relation info, and step generation state
- : List of WHERE clause expressions to process for partition pruning

## Dependencies
- Functions called/Symbols referenced:
  - partition_bound_has_default
  - [predicate_refuted_by](../p/predicate_refuted_by.md)
  - [is_orclause](../i/is_orclause.md)
  - [is_andclause](../i/is_andclause.md)
  - [match_clause_to_partition_key](../m/match_clause_to_partition_key.md)
  - [gen_prune_step_op](gen_prune_step_op.md)
  - [gen_prune_step_combine](gen_prune_step_combine.md)
  - [gen_prune_steps_from_opexps](gen_prune_steps_from_opexps.md)
  - [bms_is_member](../b/bms_is_member.md)
  - [bms_add_member](../b/bms_add_member.md)
  - bms_is_empty
  - [bms_num_members](../b/bms_num_members.md)
  - [list_concat](../l/list_concat.md)
  - [lappend_int](../l/lappend_int.md)
  - llast
- Called from (representative examples):
  - [gen_partprune_steps](gen_partprune_steps.md) (partprune.c:736)
  - [gen_partprune_steps_internal](gen_partprune_steps_internal.md) (recursive calls at lines 1043, 1114)
  - [match_clause_to_partition_key](../m/match_clause_to_partition_key.md) (partprune.c:1855, 2368)

## Notes and Other Information
- Returns NIL when contradictory clauses are found or no pruning is possible
- Supports all PostgreSQL partitioning strategies: LIST, RANGE, and HASH
- Handles special cases like default partitions and null-accepting partitions
- Automatically combines multiple steps with INTERSECT when clauses are mutually ANDed
- Uses keyclauses array indexed by partition key position to organize clause matching
- Maintains nullkeys and notnullkeys bitmapsets to track IS NULL/NOT NULL constraints
- The function is recursive for handling nested BoolExpr structures
- Memory management relies on the current memory context for temporary allocations

## Simplified Source

```c
static List *
gen_partprune_steps_internal(GeneratePruningStepsContext *context,
                             List *clauses)
{
    PartitionScheme part_scheme = context->rel->part_scheme;
    List       *keyclauses[PARTITION_MAX_KEYS];
    Bitmapset  *nullkeys = NULL, *notnullkeys = NULL;
    bool        generate_opsteps = false;
    List       *result = NIL;
    ListCell   *lc;

    // Check for contradictory clauses with partition constraint
    if (partition_bound_has_default(context->rel->boundinfo) &&
        predicate_refuted_by(context->rel->partition_qual, clauses, false))
    {
        context->contradictory = true;
        return NIL;
    }

    memset(keyclauses, 0, sizeof(keyclauses));

    // Process each clause to generate pruning steps
    foreach(lc, clauses)
    {
        Expr *clause = (Expr *) lfirst(lc);

        // Look through RestrictInfo wrapper
        if (IsA(clause, RestrictInfo))
            clause = ((RestrictInfo *) clause)->clause;

        // Constant false/null clause means contradiction
        if (IsA(clause, Const) &&
            (((Const *) clause)->constisnull ||
             !DatumGetBool(((Const *) clause)->constvalue)))
        {
            context->contradictory = true;
            return NIL;
        }

        // Handle BoolExpr clauses (OR/AND)
        if (IsA(clause, BoolExpr))
        {
            if (is_orclause(clause))
            {
                // Process OR clause: combine args with UNION
                List *arg_stepids = NIL;
                bool all_args_contradictory = true;
                ListCell *lc1;

                foreach(lc1, ((BoolExpr *) clause)->args)
                {
                    Expr *arg = lfirst(lc1);
                    List *argsteps = gen_partprune_steps_internal(context, list_make1(arg));
                    bool arg_contradictory = context->contradictory;
                    context->contradictory = false;

                    if (!arg_contradictory)
                    {
                        all_args_contradictory = false;
                        if (argsteps != NIL)
                        {
                            PartitionPruneStep *last = llast(argsteps);
                            arg_stepids = lappend_int(arg_stepids, last->step_id);
                        }
                        else
                        {
                            // Create dummy step for args that don't match partition key
                            PartitionPruneStep *orstep = gen_prune_step_combine(context, NIL,
                                                                              PARTPRUNE_COMBINE_UNION);
                            arg_stepids = lappend_int(arg_stepids, orstep->step_id);
                        }
                    }
                }

                if (all_args_contradictory)
                {
                    context->contradictory = true;
                    return NIL;
                }

                if (arg_stepids != NIL)
                {
                    PartitionPruneStep *step = gen_prune_step_combine(context, arg_stepids,
                                                                    PARTPRUNE_COMBINE_UNION);
                    result = lappend(result, step);
                }
                continue;
            }
            else if (is_andclause(clause))
            {
                // Process AND clause: combine args with INTERSECT
                List *args = ((BoolExpr *) clause)->args;
                List *argsteps = gen_partprune_steps_internal(context, args);

                if (context->contradictory)
                    return NIL;

                if (argsteps != NIL)
                    result = lappend(result, llast(argsteps));
                continue;
            }
        }

        // Try to match clause to partition keys
        for (int i = 0; i < part_scheme->partnatts; i++)
        {
            Expr *partkey = linitial(context->rel->partexprs[i]);
            bool clause_is_not_null = false;
            PartClauseInfo *pc = NULL;
            List *clause_steps = NIL;

            switch (match_clause_to_partition_key(context, clause, partkey, i,
                                                &clause_is_not_null, &pc, &clause_steps))
            {
                case PARTCLAUSE_MATCH_CLAUSE:
                    // Found matching OpExpr clause
                    if (bms_is_member(i, nullkeys))
                    {
                        context->contradictory = true;
                        return NIL;
                    }
                    generate_opsteps = true;
                    keyclauses[i] = lappend(keyclauses[i], pc);
                    break;

                case PARTCLAUSE_MATCH_NULLNESS:
                    // Found IS NULL/NOT NULL clause
                    if (!clause_is_not_null)
                    {
                        if (bms_is_member(i, notnullkeys) || keyclauses[i] != NIL)
                        {
                            context->contradictory = true;
                            return NIL;
                        }
                        nullkeys = bms_add_member(nullkeys, i);
                    }
                    else
                    {
                        if (bms_is_member(i, nullkeys))
                        {
                            context->contradictory = true;
                            return NIL;
                        }
                        notnullkeys = bms_add_member(notnullkeys, i);
                    }
                    break;

                case PARTCLAUSE_MATCH_STEPS:
                    result = list_concat(result, clause_steps);
                    break;

                case PARTCLAUSE_MATCH_CONTRADICT:
                    context->contradictory = true;
                    return NIL;

                case PARTCLAUSE_NOMATCH:
                case PARTCLAUSE_UNSUPPORTED:
                    continue;
            }
            break;
        }
    }

    // Generate pruning steps using three strategies
    if (!bms_is_empty(nullkeys) &&
        (part_scheme->strategy == PARTITION_STRATEGY_LIST ||
         part_scheme->strategy == PARTITION_STRATEGY_RANGE ||
         (part_scheme->strategy == PARTITION_STRATEGY_HASH &&
          bms_num_members(nullkeys) == part_scheme->partnatts)))
    {
        // Strategy 1: IS NULL clauses - select null-accepting partition
        PartitionPruneStep *step = gen_prune_step_op(context, InvalidStrategy,
                                                    false, NIL, NIL, nullkeys);
        result = lappend(result, step);
    }
    else if (generate_opsteps)
    {
        // Strategy 2: OpExpr-based pruning
        List *opsteps = gen_prune_steps_from_opexps(context, keyclauses, nullkeys);
        result = list_concat(result, opsteps);
    }
    else if (bms_num_members(notnullkeys) == part_scheme->partnatts)
    {
        // Strategy 3: IS NOT NULL clauses - exclude null-accepting partition
        PartitionPruneStep *step = gen_prune_step_op(context, InvalidStrategy,
                                                    false, NIL, NIL, NULL);
        result = lappend(result, step);
    }

    // Combine multiple steps with INTERSECT if needed
    if (list_length(result) > 1)
    {
        List *step_ids = NIL;
        foreach(lc, result)
        {
            PartitionPruneStep *step = lfirst(lc);
            step_ids = lappend_int(step_ids, step->step_id);
        }

        PartitionPruneStep *final = gen_prune_step_combine(context, step_ids,
                                                         PARTPRUNE_COMBINE_INTERSECT);
        result = lappend(result, final);
    }

    return result;
}
```