# gen_prune_step_combine

## Location
[src/backend/partitioning/partprune.c:1346-1382](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partprune.c#L1346-L1382)

## Overview
Generates a combination pruning step that merges the results of multiple other pruning steps using either UNION or INTERSECT operations for complex boolean logic.

## Definition

```c
static PartitionPruneStep *
gen_prune_step_combine(GeneratePruningStepsContext *context,
					   List *source_stepids,
					   PartitionPruneCombineOp combineOp)
```
## Detailed Description
This function creates a combine-type partition pruning step (PartitionPruneStepCombine) that specifies how to merge the results from multiple previously generated pruning steps. The function is essential for implementing complex boolean logic in partition pruning, particularly when dealing with OR and AND expressions in WHERE clauses.

The function allocates a new PartitionPruneStepCombine node, assigns it a unique step identifier from the context's counter, and configures it with the specified combination operation and list of source step IDs. The combine operation determines how the partition sets from the source steps should be merged:

- PARTPRUNE_COMBINE_UNION: Used for OR logic, includes partitions that appear in any of the source steps
- PARTPRUNE_COMBINE_INTERSECT: Used for AND logic, includes only partitions that appear in all source steps

The newly created step is added to the context's steps list and returned for use in the pruning step sequence.

## Parameters / Member Variables
- `*context`: GeneratePruningStepsContext containing step generation state and step counter
- `*source_stepids`: List of integers representing the step IDs of previously generated steps to be combined
- `combineOp`: PartitionPruneCombineOp specifying the combination operation (UNION or INTERSECT)
## Dependencies
- Functions called/Symbols referenced:
  - makeNode (for PartitionPruneStepCombine allocation)
  - [lappend](../l/lappend.md) (to add step to context->steps list)
- Called from (representative examples):
  - [gen_partprune_steps_internal](gen_partprune_steps_internal.md) (partprune.c:1081, 1098, 1297)

## Notes and Other Information
- Assigns unique step IDs using context->next_step_id++ for proper step sequencing
- Essential for implementing complex boolean expressions (OR/AND) in partition pruning
- The source_stepids list can be empty, which creates a special case combine step
- UNION operations are used when processing OR clauses in BoolExpr
- INTERSECT operations are used when processing AND clauses or combining multiple WHERE conditions
- The step becomes part of the overall pruning step sequence and is executed after its source steps
- Memory allocation uses makeNode which allocates in the current memory context

## Simplified Source

```c
static PartitionPruneStep *gen_prune_step_combine(GeneratePruningStepsContext *context,
                                                 List *source_stepids,
                                                 PartitionPruneCombineOp combineOp) {
    // Create new combine step node
    PartitionPruneStepCombine *cstep = makeNode(PartitionPruneStepCombine);

    // Initialize step with unique ID and parameters
    cstep->step.step_id = context->next_step_id++;
    cstep->combineOp = combineOp;  // UNION or INTERSECT
    cstep->source_stepids = source_stepids;

    // Add to context's step list
    context->steps = lappend(context->steps, cstep);

    return (PartitionPruneStep *) cstep;
}
```