# get_steps_using_prefix_recurse

## Location
src/backend/partitioning/partprune.c: 2496 - 2662

## Overview
Recursively generates all valid combinations of PartitionPruneStepOps when multiple PartClauseInfos exist for the same partition key.

## Definition


## Detailed Description
This function implements the recursive core of multi-key partition pruning step generation. It handles the complex case where multiple PartClauseInfos exist for the same partition key, creating a cartesian product of all valid combinations. The recursion proceeds by:

1. Finding all PartClauseInfos for the current partition key
2. For each clause, recursively processing remaining keys with updated expression/function lists
3. At the final recursion level, generating actual pruning steps by combining all accumulated expressions

The function maintains careful bookkeeping of expressions and comparison functions for each partition key, ensuring that the final pruning steps contain exactly one expression per partition key. Special handling exists for hash partitioning where NULL keys are allowed via the step_nullkeys bitmapset.

## Parameters / Member Variables
- : Context information for generating pruning steps
- : Strategy number for the comparison operation
- : Boolean indicating if this is a not-equal operation
- : Expression for the final partition key
- : Comparison function OID for the final partition key
- : Bitmapset indicating which keys should be treated as NULL
- : List of PartClauseInfos sorted by keyno
- : Starting point in the prefix list for this recursion level
- : Accumulated expressions from previous partition keys
- : Accumulated comparison functions from previous partition keys

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth
  - lfirst
  - llast
  - for_each_cell
  - list_copy
  - lappend
  - lappend_oid
  - list_concat
  - list_free
  - gen_prune_step_op
  - bms_is_empty
  - bms_num_members
- Called from:
  - get_steps_using_prefix
  - get_steps_using_prefix_recurse (recursive)

## Notes and Other Information
The function includes important assertions to validate the structure for hash partitioning, ensuring that each partition key has either an equality clause or is marked as NULL in step_nullkeys. The recursion is bounded by PARTITION_MAX_KEYS to prevent stack overflow. Memory management is carefully handled by copying and freeing intermediate expression/function lists to avoid modifying shared data structures. This function is critical for optimizing queries with complex multi-column partition key predicates involving multiple clauses per key.