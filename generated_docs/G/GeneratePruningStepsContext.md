# GeneratePruningStepsContext

## Location
src/backend/partitioning/partprune.c: 110 - 124

## Overview
GeneratePruningStepsContext is a structure that maintains the current state during the generation of partition pruning steps for a given set of clauses.

## Definition
```c
typedef struct GeneratePruningStepsContext
{
    /* Copies of input arguments for gen_partprune_steps: */
    RelOptInfo *rel;            /* the partitioned relation */
    PartClauseTarget target;    /* use-case we're generating steps for */
    /* Result data: */
    List       *steps;          /* list of PartitionPruneSteps */
    bool        has_mutable_op; /* clauses include any stable operators */
    bool        has_mutable_arg;/* clauses include any mutable comparison
                                 * values, *other than* exec params */
    bool        has_exec_param; /* clauses include any PARAM_EXEC params */
    bool        contradictory;  /* clauses were proven self-contradictory */
    /* Working state: */
    int         next_step_id;
} GeneratePruningStepsContext;
```

## Detailed Description
GeneratePruningStepsContext is a comprehensive context structure used during the generation of partition pruning steps in PostgreSQL's partition elimination system, defined in src/backend/partitioning/partprune.c:110-124. This structure serves as both input container and result accumulator for the complex process of converting WHERE clause conditions into executable pruning steps.

The context tracks various properties of the clauses being processed, including whether they contain mutable operators, mutable arguments, or execution parameters. This information is crucial for determining when pruning steps can be cached versus when they must be re-evaluated. The structure also detects contradictory conditions that would result in no matching partitions.

The context is initialized by gen_partprune_steps() and passed through the pruning step generation pipeline, accumulating results and maintaining state as different types of conditions are processed and converted into pruning operations.

## Parameters / Member Variables
- `rel`: Pointer to the RelOptInfo structure representing the partitioned relation being processed
- `target`: Specifies the use-case context for which pruning steps are being generated (of type PartClauseTarget)
- `steps`: A list containing the generated PartitionPruneStep structures that represent the pruning operations
- `has_mutable_op`: Boolean flag indicating whether any processed clauses contain stable (non-immutable) operators
- `has_mutable_arg`: Boolean flag indicating whether clauses contain mutable comparison values, excluding execution parameters
- `has_exec_param`: Boolean flag indicating whether clauses include any PARAM_EXEC parameters
- `contradictory`: Boolean flag set when the clauses are determined to be self-contradictory (no partitions can match)
- `next_step_id`: Working counter used to assign unique identifiers to generated pruning steps

## Dependencies
- Functions called/Symbols referenced:
  - RelOptInfo (relation optimization info structure)
  - PartClauseTarget (enumeration for pruning target context)
  - [List](../L/List.md) (PostgreSQL list structure)
  - PartitionPruneStep (pruning step structure)

- Called from (representative examples):
  - [make_partitionedrel_pruneinfo](../m/make_partitionedrel_pruneinfo.md)
  - [gen_partprune_steps](../g/gen_partprune_steps.md)
  - [prune_append_rel_partitions](../p/prune_append_rel_partitions.md)
  - [gen_partprune_steps_internal](../g/gen_partprune_steps_internal.md)
  - [gen_prune_step_op](../g/gen_prune_step_op.md)
  - [gen_prune_step_combine](../g/gen_prune_step_combine.md)
  - [gen_prune_steps_from_opexps](../g/gen_prune_steps_from_opexps.md)
  - [match_clause_to_partition_key](../m/match_clause_to_partition_key.md)
  - [get_steps_using_prefix](../g/get_steps_using_prefix.md)
  - [get_steps_using_prefix_recurse](../g/get_steps_using_prefix_recurse.md)

## Notes and Other Information
- The has_mutable_op, has_mutable_arg, and has_exec_param flags are set if any potentially-useful-for-pruning clause has those properties, regardless of whether the clause was actually used in the final steps list
- This definition of the mutability flags allows the system to skip the PARTTARGET_EXEC pass in certain optimization scenarios
- The contradictory flag provides early detection of impossible conditions, allowing the pruning system to immediately eliminate all partitions
- The structure is central to PostgreSQL's partition-wise optimization, which can dramatically improve query performance on partitioned tables
- The next_step_id field ensures that each generated pruning step has a unique identifier for tracking and debugging purposes
- This context structure exemplifies PostgreSQL's approach to complex optimization tasks: maintaining comprehensive state while processing and accumulating results incrementally