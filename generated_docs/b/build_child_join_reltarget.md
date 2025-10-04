# build_child_join_reltarget

## Location
[src/backend/optimizer/util/relnode.c:2429-2445](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/relnode.c#L2429-L2445)

## Overview
Sets up the target list (output columns) for a child join relation by adapting the parent join relation's target list using append relation mappings.

## Definition

```c
static void
build_child_join_reltarget(PlannerInfo *root,
						   RelOptInfo *parentrel,
						   RelOptInfo *childrel,
						   int nappinfos,
						   AppendRelInfo **appinfos)
```
## Detailed Description
This function constructs the reltarget (relation target list) for a child join relation in partitionwise join processing. It takes the parent join relation's target expressions and transforms them to reference the appropriate child relations using append relation information. The function performs three main operations:

1. **Expression Translation**: Uses  to translate the parent's target list expressions, replacing references to parent relations with references to their corresponding child partitions.

2. **Cost Propagation**: Copies cost estimates (startup and per-tuple costs) directly from the parent relation, assuming that child relations have similar cost characteristics.

3. **Width Propagation**: Copies the estimated output width from the parent relation to the child relation.

This is a critical component of partitionwise join optimization, ensuring that child join relations have properly configured target lists that reference the correct partition-specific tables while maintaining cost and selectivity estimates for accurate planning.

## Parameters / Member Variables
- `*root`: PlannerInfo containing global planner state and transformation context
- `*parentrel`: The parent join relation whose reltarget serves as the template
- `*childrel`: The child join relation being constructed that will receive the adapted reltarget
- `nappinfos`: Number of AppendRelInfo structures in the appinfos array
- `**appinfos`: Array of AppendRelInfo structures containing parent-to-child relation mappings
## Dependencies
- Functions called/Symbols referenced:
  - : Transforms expressions to replace parent relation references with child relation references
  - : Structure containing mapping information between parent and child relations
- Called from (representative examples):
  - : Main child join relation construction function during partitionwise join processing

## Notes and Other Information
- This function is part of PostgreSQL's partitionwise join optimization infrastructure
- The cost and width estimates are inherited directly from the parent without adjustment, based on the assumption that partitioned child relations have similar characteristics
- The AppendRelInfo array provides the necessary mapping to translate Var references from parent relation attribute numbers to child relation attribute numbers
- Essential for ensuring that partitionwise joins produce correctly structured output with proper column references to child partitions rather than abstract parent tables

## Simplified Source

```c
static void
build_child_join_reltarget(PlannerInfo *root,
                           RelOptInfo *parentrel,
                           RelOptInfo *childrel,
                           int nappinfos,
                           AppendRelInfo **appinfos)
{
    // Translate parent's target expressions to reference child relations
    childrel->reltarget->exprs = (List *)
        adjust_appendrel_attrs(root,
                               (Node *) parentrel->reltarget->exprs,
                               nappinfos, appinfos);

    // Copy cost and width estimates from parent
    childrel->reltarget->cost.startup = parentrel->reltarget->cost.startup;
    childrel->reltarget->cost.per_tuple = parentrel->reltarget->cost.per_tuple;
    childrel->reltarget->width = parentrel->reltarget->width;
}
```