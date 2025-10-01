# set_joinrel_partition_key_exprs

## Location
[src/backend/optimizer/util/relnode.c:2285-2428](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/relnode.c#L2285-L2428)

## Overview
Initializes partition key expressions for a partitioned join relation by determining which partition keys remain valid and nullable based on the specific join type being performed.

## Definition

```c
structures that
				 * parse analysis would build for such variables.  But it's
				 * hard to tell which those are, so just make all the pairs.
				 * Extra items in the nullable_partexprs list won't cause big
				 * problems.  (It's possible that such items will get matched
				 * to user-written COALESCEs, but it should still be valid to
				 * partition on those, since they're going to be either the
				 * partition column or NULL;
```
## Detailed Description
This function constructs the partition key expressions for a join relation by analyzing how different join types affect the nullability and validity of partition keys from the input relations. It allocates arrays for both regular () and nullable () partition key expressions and populates them according to join semantics:

**INNER JOIN**: Both outer and inner partition keys remain valid since no NULLs are introduced. Previously nullable keys remain nullable.

**SEMI/ANTI JOIN**: Only outer relation keys are preserved since inner relation columns are not visible in the output.

**LEFT OUTER JOIN**: Outer keys remain non-nullable, while inner keys become nullable due to potential NULL-padding when no match exists.

**FULL OUTER JOIN**: All keys from both relations become nullable. Additionally, COALESCE expressions are generated for all combinations of outer and inner partition keys to handle merged columns from JOIN USING clauses.

The function handles the complex task of maintaining partition key validity across different join types, which is essential for enabling further partitionwise join optimizations on the resulting join relation.

## Parameters / Member Variables
- : The join relation being constructed that will receive the partition key expressions
- : The outer (left) input relation with existing partition key expressions
- : The inner (right) input relation with existing partition key expressions  
- : The type of join operation (INNER, LEFT, SEMI, ANTI, FULL) determining key handling strategy

## Dependencies
- Functions called/Symbols referenced:
  - : Creates concatenated copies of partition key expression lists
  - : Creates copies of partition key expression lists
  - : Concatenates partition key expression lists in-place
  - : Creates two-element list for COALESCE expression arguments
  - : Allocates new CoalesceExpr nodes for FULL JOIN
  - : Determines data type of partition key expressions
  - : Determines collation of partition key expressions
  - : Appends expressions to nullable partition key lists
  - : Expression node for COALESCE operations in FULL JOINs
  - , , , , : Join type constants
- Called from (representative examples):
  - : Main partition info setup function after validating partitionwise join feasibility

## Notes and Other Information
- Allocates memory for  partition key positions using  for zero-initialized arrays
- For FULL JOINs, generates COALESCE expressions for all combinations of outer and inner partition keys to handle JOIN USING semantics
- Intentionally omits varnullingrels decoration from COALESCE expressions since  strips them during comparison
- The distinction between nullable and non-nullable partition keys is crucial for maintaining correctness in subsequent partitionwise join decisions
- COALESCE expressions in FULL JOINs enable matching of equijoin conditions that reference merged columns, expanding partitionwise join opportunities

## Simplified Source

```c
static void set_joinrel_partition_key_exprs(RelOptInfo *joinrel,
                                           RelOptInfo *outer_rel,
                                           RelOptInfo *inner_rel,
                                           JoinType jointype) {
    PartitionScheme part_scheme = joinrel->part_scheme;
    int partnatts = part_scheme->partnatts;

    // Allocate arrays for partition key expressions
    joinrel->partexprs = (List **) palloc0(sizeof(List *) * partnatts);
    joinrel->nullable_partexprs = (List **) palloc0(sizeof(List *) * partnatts);

    // Process each partition key position
    for (int cnt = 0; cnt < partnatts; cnt++) {
        const List *outer_expr = outer_rel->partexprs[cnt];
        const List *outer_null_expr = outer_rel->nullable_partexprs[cnt];
        const List *inner_expr = inner_rel->partexprs[cnt];
        const List *inner_null_expr = inner_rel->nullable_partexprs[cnt];

        List *partexpr = NIL;
        List *nullable_partexpr = NIL;

        switch (jointype) {
            case JOIN_INNER:
                // Both sides remain valid, preserve nullability
                partexpr = list_concat_copy(outer_expr, inner_expr);
                nullable_partexpr = list_concat_copy(outer_null_expr, inner_null_expr);
                break;

            case JOIN_SEMI:
            case JOIN_ANTI:
                // Only outer side visible in output
                partexpr = list_copy(outer_expr);
                nullable_partexpr = list_copy(outer_null_expr);
                break;

            case JOIN_LEFT:
                // Outer non-nullable, inner becomes nullable
                partexpr = list_copy(outer_expr);
                nullable_partexpr = list_concat_copy(inner_expr, outer_null_expr);
                nullable_partexpr = list_concat(nullable_partexpr, inner_null_expr);
                break;

            case JOIN_FULL:
                // All keys become nullable
                nullable_partexpr = list_concat_copy(outer_expr, inner_expr);
                nullable_partexpr = list_concat(nullable_partexpr, outer_null_expr);
                nullable_partexpr = list_concat(nullable_partexpr, inner_null_expr);

                // Add COALESCE expressions for JOIN USING support
                add_coalesce_expressions_for_full_join(&nullable_partexpr,
                                                      outer_expr, outer_null_expr,
                                                      inner_expr, inner_null_expr);
                break;
        }

        joinrel->partexprs[cnt] = partexpr;
        joinrel->nullable_partexprs[cnt] = nullable_partexpr;
    }
}
```