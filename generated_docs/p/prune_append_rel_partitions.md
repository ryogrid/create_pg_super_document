# prune_append_rel_partitions

## Location
[src/backend/partitioning/partprune.c:750-816](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partprune.c#L750-L816)

## Overview
Performs compile-time partition pruning by processing base restriction clauses to determine the minimum set of partitions that must be scanned.

## Definition
```c
Bitmapset *prune_append_rel_partitions(RelOptInfo *rel)
```

## Detailed Description
This function implements planning-time partition pruning for partitioned tables. It analyzes the base restriction clauses (baserestrictinfo) associated with a partitioned relation and determines which partitions can be eliminated from consideration based on compile-time evaluable conditions.

The function follows this process:
1. **Early returns**: Handles edge cases like no partitions, disabled pruning, or no clauses
2. **Step generation**: Uses gen_partprune_steps to convert restriction clauses into pruning steps for planner-time evaluation (PARTTARGET_PLANNER)
3. **Contradiction detection**: Returns empty set if clauses are contradictory
4. **Context setup**: Initializes PartitionPruneContext with partition scheme information
5. **Pruning execution**: Calls get_matching_partitions to perform the actual pruning

The function only considers immutable expressions and clauses that can be evaluated at plan time, ensuring that the pruning decisions are stable and don't depend on runtime parameter values.

## Parameters / Member Variables
- `rel`: RelOptInfo for the partitioned table being pruned (must have part_scheme != NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [gen_partprune_steps](../g/gen_partprune_steps.md)
  - [get_matching_partitions](../g/get_matching_partitions.md)
  - [bms_add_range](../b/bms_add_range.md)
  - [palloc0](palloc0.md)
- Called from (representative examples):
  - [expand_partitioned_rtentry](../e/expand_partitioned_rtentry.md)

## Notes and Other Information
- Returns a Bitmapset containing partition indexes in the rel's part_rels array
- Returns NULL when no partitions need to be scanned (contradictory clauses)
- Returns all partitions when pruning is disabled or no useful clauses exist
- Only processes clauses that can be evaluated at planning time (immutable expressions)
- Sets up a PartitionPruneContext with planning-specific values (planstate, exprcontext, exprstates all NULL)
- Requires enable_partition_pruning to be enabled for pruning to occur
- The caller must ensure that the relation is actually partitioned before calling this function
- Uses CurrentMemoryContext for temporary allocations during pruning

## Simplified Source

```c
Bitmapset *
prune_append_rel_partitions(RelOptInfo *rel)
{
    List *clauses = rel->baserestrictinfo;
    List *pruning_steps;
    GeneratePruningStepsContext gcontext;
    PartitionPruneContext context;

    Assert(rel->part_scheme != NULL);

    // Return empty set if no partitions exist
    if (rel->nparts == 0)
        return NULL;

    // Return all partitions if pruning disabled or no clauses
    if (!enable_partition_pruning || clauses == NIL)
        return bms_add_range(NULL, 0, rel->nparts - 1);

    // Generate pruning steps from restriction clauses
    gen_partprune_steps(rel, clauses, PARTTARGET_PLANNER, &gcontext);

    // Return empty set if clauses are contradictory
    if (gcontext.contradictory)
        return NULL;

    pruning_steps = gcontext.steps;

    // Return all partitions if no usable pruning steps
    if (pruning_steps == NIL)
        return bms_add_range(NULL, 0, rel->nparts - 1);

    // Set up pruning context with partition scheme information
    context.strategy = rel->part_scheme->strategy;
    context.partnatts = rel->part_scheme->partnatts;
    context.nparts = rel->nparts;
    context.boundinfo = rel->boundinfo;
    context.partcollation = rel->part_scheme->partcollation;
    context.partsupfunc = rel->part_scheme->partsupfunc;
    context.stepcmpfuncs = (FmgrInfo *) palloc0(sizeof(FmgrInfo) *
                                               context.partnatts *
                                               list_length(pruning_steps));
    context.ppccontext = CurrentMemoryContext;

    // Planner-time context (no runtime state)
    context.planstate = NULL;
    context.exprcontext = NULL;
    context.exprstates = NULL;

    // Perform the actual partition pruning
    return get_matching_partitions(&context, pruning_steps);
}
```