# have_partkey_equi_join

## Location
[src/backend/optimizer/util/relnode.c:2090-2235](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/relnode.c#L2090-L2235)

## Overview
Determines whether there exist equi-join conditions involving pairs of matching partition keys between two relations for all partition keys, enabling partitionwise join optimization.

## Definition

```c
static bool
have_partkey_equi_join(PlannerInfo *root, RelOptInfo *joinrel,
					   RelOptInfo *rel1, RelOptInfo *rel2,
					   JoinType jointype, List *restrictlist)
```
## Detailed Description
This function analyzes join restriction clauses to determine if partitionwise join is feasible between two partitioned relations. It performs a comprehensive validation of join conditions to ensure that:

1. Every partition key position has a corresponding equi-join condition
2. The join clauses use equality operators compatible with the partition scheme
3. Join operators match the partitioning strategy (hash vs range/list partitioning)  
4. Collation requirements are satisfied for string-based partition keys
5. Proper handling of nullable expressions in outer join contexts

The function iterates through all restriction clauses, filtering for equality conditions that can be used for joins, and maps expressions to partition key positions using . For hash-partitioned tables, it validates hash join operators; for range/list partitioned tables, it checks merge join operator families. Special care is taken to handle nulling relations that may arise from outer joins by removing nulling relation IDs when the join operator is strict.

## Parameters / Member Variables
- : PlannerInfo containing global planner state including outer join information
- : The join relation being constructed (used for relid validation)
- : First relation in the join operation with partitioning information
- : Second relation in the join operation (must have same partition scheme as rel1)
- : Type of join (INNER, LEFT, RIGHT, FULL) affecting clause processing
- : List of RestrictInfo nodes containing join conditions to analyze

## Dependencies
- Functions called/Symbols referenced:
  - : Maps expressions to partition key positions
  - : Determines if join operator is strict (null-rejecting)
  - : Removes nulling relation markers for strict operators
  - : Checks if relation IDs are subsets for clause validation
  - : Checks for overlapping relation sets in outer join handling
  - : Validates operator family membership for hash partitioning
  - : Checks merge operator family membership
  - : Macro to identify outer join types
  - : Macro to check if clause is pushed down
  - : Constant for hash partitioning strategy
  - : Maximum number of partition key columns
- Called from (representative examples):
  - : Main partition info setup function

## Notes and Other Information
- Requires both relations to have identical partition schemes (assertion enforced)
- Uses boolean array  to track which partition key positions have matching equi-join conditions
- Handles special cases for outer joins by filtering pushed-down clauses and managing nulling relations
- Critical for PostgreSQL's partitionwise join optimization which can dramatically improve performance on partitioned tables
- The function ensures that partitionwise join will produce correct results by validating that all partition boundaries align properly through equi-join conditions

## Simplified Source

```c
static bool have_partkey_equi_join(PlannerInfo *root, RelOptInfo *joinrel,
                                  RelOptInfo *rel1, RelOptInfo *rel2,
                                  JoinType jointype, List *restrictlist) {
    PartitionScheme part_scheme = rel1->part_scheme;
    bool pk_has_clause[PARTITION_MAX_KEYS];

    // Both relations must have same partitioning scheme
    Assert(rel1->part_scheme == rel2->part_scheme);

    // Initialize tracking array for partition keys
    memset(pk_has_clause, 0, sizeof(pk_has_clause));

    // Check each restriction clause for partition key equi-joins
    foreach(lc, restrictlist) {
        RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

        // Skip inappropriate clauses for outer joins
        if (IS_OUTER_JOIN(jointype) && RINFO_IS_PUSHED_DOWN(rinfo, joinrel->relids))
            continue;

        // Only process equality join clauses
        if (!rinfo->can_join ||
            (!rinfo->mergeopfamilies && !OidIsValid(rinfo->hashjoinoperator)))
            continue;

        OpExpr *opexpr = castNode(OpExpr, rinfo->clause);

        // Match operands to relations and extract expressions
        Expr *expr1, *expr2;
        if (!match_operands_to_relations(rinfo, rel1, rel2, &expr1, &expr2))
            continue;

        // Handle nulling relations for strict operators
        bool strict_op = op_strict(opexpr->opno);
        if (strict_op) {
            expr1 = remove_nulling_relids_if_needed(expr1, rel1, root);
            expr2 = remove_nulling_relids_if_needed(expr2, rel2, root);
        }

        // Check if expressions match partition keys at same position
        int ipk1 = match_expr_to_partition_keys(expr1, rel1, strict_op);
        int ipk2 = match_expr_to_partition_keys(expr2, rel2, strict_op);
        if (ipk1 < 0 || ipk2 < 0 || ipk1 != ipk2)
            continue;

        // Validate collation and operator family compatibility
        if (!validate_partition_join_compatibility(rel1, opexpr, rinfo, ipk1))
            continue;

        // Mark this partition key as having an equi-join clause
        pk_has_clause[ipk1] = true;
    }

    // Verify all partition keys have equi-join conditions
    for (int cnt_pks = 0; cnt_pks < part_scheme->partnatts; cnt_pks++) {
        if (!pk_has_clause[cnt_pks])
            return false;
    }

    return true;
}
```