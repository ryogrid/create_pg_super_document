# gen_prune_steps_from_opexps

## Location
[src/backend/partitioning/partprune.c:1383-1754](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partprune.c#L1383-L1754)

## Overview
Generates a list of PartitionPruneStepOp based on OpExpr and BooleanTest clauses that have been matched to partition keys, creating pruning steps optimized for different partitioning strategies.

## Definition

```c
static List *
gen_prune_steps_from_opexps(GeneratePruningStepsContext *context,
							List **keyclauses, Bitmapset *nullkeys)
```
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
- `*context`: GeneratePruningStepsContext containing partition metadata and step generation state
- `**keyclauses`: Array of List pointers indexed by partition key number, each containing PartClauseInfo for that key
- `*nullkeys`: Bitmapset indicating which partition keys have IS NULL clauses
## Dependencies
- Functions called/Symbols referenced:
  - [get_op_opfamily_properties](get_op_opfamily_properties.md)
  - [get_steps_using_prefix](get_steps_using_prefix.md)
  - [list_concat](../l/list_concat.md)
  - [lappend](../l/lappend.md)
  - [list_head](../l/list_head.md)
  - llast
  - for_each_cell
  - [bms_is_member](../b/bms_is_member.md)
  - BTMaxStrategyNumber, HTMaxStrategyNumber
  - HTEqualStrategyNumber, BTEqualStrategyNumber, BTLessStrategyNumber, etc.
- Called from (representative examples):
  - [gen_partprune_steps_internal](gen_partprune_steps_internal.md) (partprune.c:1267)

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

## Simplified Source

```c
static List *gen_prune_steps_from_opexps(GeneratePruningStepsContext *context,
                                        List **keyclauses, Bitmapset *nullkeys) {
    PartitionScheme part_scheme = context->rel->part_scheme;
    List *opsteps = NIL;
    List *btree_clauses[BTMaxStrategyNumber + 1];
    List *hash_clauses[HTMaxStrategyNumber + 1];

    // Initialize clause arrays
    memset(btree_clauses, 0, sizeof(btree_clauses));
    memset(hash_clauses, 0, sizeof(hash_clauses));

    // Phase 1: Organize clauses by strategy and validate constraints
    for (int i = 0; i < part_scheme->partnatts; i++) {
        List *clauselist = keyclauses[i];

        // Range partitioning: stop if no clauses for current key
        if (part_scheme->strategy == PARTITION_STRATEGY_RANGE && clauselist == NIL)
            break;

        // Hash partitioning: require equality or null clauses for all keys
        if (part_scheme->strategy == PARTITION_STRATEGY_HASH &&
            clauselist == NIL && !bms_is_member(i, nullkeys))
            return NIL;

        // Categorize clauses by strategy
        foreach(lc, clauselist) {
            PartClauseInfo *pc = (PartClauseInfo *) lfirst(lc);

            // Discover operator strategy if needed
            if (pc->op_strategy == InvalidStrategy)
                get_op_opfamily_properties(pc->opno, part_scheme->partopfamily[i],
                                         false, &pc->op_strategy, &lefttype, &righttype);

            // Add to appropriate strategy array
            switch (part_scheme->strategy) {
                case PARTITION_STRATEGY_LIST:
                case PARTITION_STRATEGY_RANGE:
                    btree_clauses[pc->op_strategy] = lappend(btree_clauses[pc->op_strategy], pc);
                    break;
                case PARTITION_STRATEGY_HASH:
                    if (pc->op_strategy != HTEqualStrategyNumber)
                        elog(ERROR, "invalid clause for hash partitioning");
                    hash_clauses[pc->op_strategy] = lappend(hash_clauses[pc->op_strategy], pc);
                    break;
            }
        }
    }

    // Phase 2: Generate steps based on partitioning strategy
    switch (part_scheme->strategy) {
        case PARTITION_STRATEGY_LIST:
        case PARTITION_STRATEGY_RANGE:
            // Process each btree strategy (=, <=, >=, <, >)
            for (int strat = 1; strat <= BTMaxStrategyNumber; strat++) {
                foreach(lc, btree_clauses[strat]) {
                    PartClauseInfo *pc = lfirst(lc);

                    // Build prefix from earlier keys and generate steps
                    if (pc->keyno == 0) {
                        // First key - no prefix needed
                        List *pc_steps = get_steps_using_prefix(context, strat,
                                                              pc->op_is_ne, pc->expr,
                                                              pc->cmpfn, NULL, NIL);
                        opsteps = list_concat(opsteps, pc_steps);
                    } else {
                        // Build prefix and generate steps if valid
                        List *prefix = build_prefix_for_strategy(btree_clauses, pc, strat);
                        if (prefix_is_valid(prefix, pc->keyno)) {
                            List *pc_steps = get_steps_using_prefix(context, strat,
                                                                  pc->op_is_ne, pc->expr,
                                                                  pc->cmpfn, NULL, prefix);
                            opsteps = list_concat(opsteps, pc_steps);
                        }
                    }
                }
            }
            break;

        case PARTITION_STRATEGY_HASH:
            // Hash partitioning - only equality strategy
            List *eq_clauses = hash_clauses[HTEqualStrategyNumber];
            if (eq_clauses != NIL) {
                // Find clauses for last key and build prefix from earlier keys
                PartClauseInfo *last_pc = llast(eq_clauses);
                List *prefix = build_hash_prefix(eq_clauses, last_pc->keyno);

                // Generate steps for each clause of the last key
                foreach(lc, eq_clauses) {
                    PartClauseInfo *pc = lfirst(lc);
                    if (pc->keyno == last_pc->keyno) {
                        List *pc_steps = get_steps_using_prefix(context, HTEqualStrategyNumber,
                                                              false, pc->expr, pc->cmpfn,
                                                              nullkeys, prefix);
                        opsteps = list_concat(opsteps, pc_steps);
                    }
                }
            }
            break;
    }

    return opsteps;
}
```