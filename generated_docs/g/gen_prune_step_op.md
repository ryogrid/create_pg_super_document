# gen_prune_step_op

## Location
src/backend/partitioning/partprune.c: 1313 - 1345

## Overview
Generates a pruning step for a specific operator by creating a PartitionPruneStepOp node with operator strategy, expressions, comparison functions, and null key information.

## Definition


## Detailed Description
This function creates an operator-based partition pruning step (PartitionPruneStepOp) that encapsulates the information needed to perform partition elimination based on a specific comparison operator. The function allocates a new step node, assigns it a unique step identifier from the context, and configures it with the provided operator strategy, expressions, comparison functions, and null key constraints.

A special case is handled for inequality operators (<>): when op_is_ne is true, the function sets opstrategy to InvalidStrategy to signal the execution code (specifically get_matching_list_bounds) to handle the NOT EQUAL operation appropriately. The function ensures that the number of expressions matches the number of comparison functions through an assertion.

The newly created step is added to the context's steps list and returned as a PartitionPruneStep pointer for use in the pruning step sequence.

## Parameters / Member Variables
- : GeneratePruningStepsContext containing step generation state and step counter
- : StrategyNumber indicating the comparison strategy (e.g., BTLessStrategyNumber, BTEqualStrategyNumber)
- : Boolean flag indicating if this is a NOT EQUAL (<>) operator
- : List of expressions being compared to partition keys
- : List of comparison functions corresponding to each expression
- : Bitmapset indicating which partition keys should be treated as null

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (for PartitionPruneStepOp allocation)
  - lappend (to add step to context->steps list)
  - InvalidStrategy (special strategy value for <> operators)
  - list_length (for assertion checking)
- Called from (representative examples):
  - [gen_partprune_steps_internal](gen_partprune_steps_internal.md) (partprune.c:1258, 1275)
  - [get_steps_using_prefix](get_steps_using_prefix.md) (partprune.c:2458)
  - [get_steps_using_prefix_recurse](get_steps_using_prefix_recurse.md) (partprune.c:2628)

## Notes and Other Information
- Assigns unique step IDs using context->next_step_id++ for proper step sequencing
- Handles the special case of NOT EQUAL operators by setting opstrategy to InvalidStrategy
- Maintains strict correspondence between expressions and comparison functions lists
- The returned step becomes part of the overall pruning step sequence for partition elimination
- Memory allocation uses makeNode which allocates in the current memory context
- The step is automatically added to the context's step list for later execution