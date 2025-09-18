# gen_prune_steps_from_opexps

## Location
src/backend/partitioning/partprune.c: 1383 - 1754

## Overview
Generates a list of PartitionPruneStepOp based on OpExpr and BooleanTest clauses that have been matched to partition keys, creating pruning steps optimized for different partitioning strategies.

## Definition


## Detailed Description
This function is responsible for converting matched operator clauses into concrete partition pruning steps. It processes an array of clause lists indexed by partition key position and generates appropriate pruning steps based on the partitioning strategy (LIST, RANGE, or HASH).

The function operates in two main phases:

**Phase 1: Clause Organization**
- Separates clauses by operator strategy into btree_clauses and hash_clauses arrays
- For RANGE partitioning, stops processing when a key has no clauses (prefix requirement)
- For HASH partitioning, requires either equality clauses or IS NULL clauses for all keys
- Validates operator strategies and handles strategy discovery for clauses

**Phase 2: Step Generation by Strategy**
- **LIST/RANGE**: Processes clauses by btree strategy (=, <=, >=, <, >), building prefix expressions from earlier keys and generating steps for each valid combination
- **HASH**: Processes only equality clauses, requiring complete key coverage, and generates steps with proper null key handling

For LIST and RANGE partitioning, the function implements sophisticated prefix logic where clauses for earlier partition keys form a "prefix" that constrains the search space for later keys. It handles complex scenarios with multiple clauses per key and ensures proper ordering based on operator inclusiveness.

## Parameters / Member Variables
- : GeneratePruningStepsContext containing partition metadata and step generation state
- : Array of List pointers indexed by partition key number, each containing PartClauseInfo for that key
- : Bitmapset indicating which partition keys have IS NULL clauses

## Dependencies
- Functions called/Symbols referenced:
  - get_op_opfamily_properties
  - get_steps_using_prefix
  - list_concat
  - lappend
  - list_head
  - llast
  - for_each_cell
  - bms_is_member
  - BTMaxStrategyNumber, HTMaxStrategyNumber
  - HTEqualStrategyNumber, BTEqualStrategyNumber, BTLessStrategyNumber, etc.
- Called from (representative examples):
  - gen_partprune_steps_internal (partprune.c:1267)

## Notes and Other Information
- Returns NIL when no useful pruning steps can be generated
- For HASH partitioning, equality clauses are required for all partition keys (or IS NULL clauses)
- For RANGE partitioning, supports partial key matching but requires contiguous prefix coverage
- Handles complex multi-key scenarios with multiple clauses per partition key
- The function does not add combine steps - caller is responsible for combining returned steps
- Strategy-specific optimizations: RANGE allows prefix matching, HASH requires complete key coverage
- Validates operator strategies and discovers them dynamically when needed
- Memory management relies on the current memory context for temporary allocations
- The prefix logic ensures that generated steps respect partition key ordering requirements