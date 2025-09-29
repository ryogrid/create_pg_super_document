# get_qual_for_range

## Location
[src/backend/partitioning/partbounds.c:4275-4631](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L4275-L4631)

## Overview
Generates complex CHECK constraint expressions for range partitions that handle multi-column range boundaries with proper comparison logic and special handling for MINVALUE/MAXVALUE bounds.

## Definition
static List *get_qual_for_range(Relation parent, PartitionBoundSpec *spec, bool for_default)

## Detailed Description
This function constructs sophisticated partition constraints for range partitions, supporting both single and multi-column range keys. For multi-column keys, it generates optimized expression trees that handle lexicographic ordering correctly.

For a multi-column range partition key (a, b, c) with lower bound (al, bl, cl) and upper bound (au, bu, cu), it generates expressions like:
- (a IS NOT NULL) AND (b IS NOT NULL) AND (c IS NOT NULL)
- AND (a > al OR (a = al AND b > bl) OR (a = al AND b = bl AND c >= cl))  
- AND (a < au OR (a = au AND b < bu) OR (a = au AND b = bu AND c < cu))

The function optimizes cases where prefixes of bounds are equal, and handles MINVALUE/MAXVALUE bounds by simplifying expressions appropriately. For default partitions, it recursively collects constraints from all other partitions and returns their negation.

## Parameters / Member Variables
- parent: The parent relation that is being partitioned
- spec: Partition bound specification containing lower and upper bound datums
- for_default: Whether this is a recursive call for generating default partition constraints

## Dependencies
- Functions called/Symbols referenced:
  - [RelationGetPartitionKey](../R/RelationGetPartitionKey.md)
  - [RelationGetPartitionDesc](../R/RelationGetPartitionDesc.md)
  - [get_range_nulltest](get_range_nulltest.md)
  - [get_range_key_properties](get_range_key_properties.md)
  - [CreateExecutorState](../C/CreateExecutorState.md)
  - [make_partition_op_expr](../m/make_partition_op_expr.md)
  - [fix_opfuncids](../f/fix_opfuncids.md)
  - [ExecInitExpr](../E/ExecInitExpr.md)
  - [ExecEvalExprSwitchContext](../E/ExecEvalExprSwitchContext.md)
  - [FreeExecutorState](../F/FreeExecutorState.md)
  - [makeBoolExpr](../m/makeBoolExpr.md)
  - [makeBoolConst](../m/makeBoolConst.md)
- Called from (representative examples):
  - [get_qual_from_partbound](get_qual_from_partbound.md)
  - [check_default_partition_contents](../c/check_default_partition_contents.md)
  - [get_qual_for_range](get_qual_for_range.md) (recursive for default partitions)

## Notes and Other Information
- Handles both single and multi-column range partitioning with complex lexicographic comparisons
- Optimizes expressions when bound prefixes are equal by generating equality constraints
- Properly handles MINVALUE and MAXVALUE bounds by simplifying comparisons
- For default partitions, uses recursive calls to collect all other partition constraints
- Uses executor state to evaluate equality tests between lower and upper bounds
- Generates NOT NULL tests for all partition key columns unless in recursive default mode
- The constraint generation ensures proper row distribution and enables constraint exclusion during query planning

## Simplified Source

```c
static List *get_qual_for_range(Relation parent, PartitionBoundSpec *spec, bool for_default) {
    List *result = NIL;
    PartitionKey key = RelationGetPartitionKey(parent);

    if (spec->is_default) {
        // For default partition, collect constraints from all other partitions
        PartitionDesc pdesc = RelationGetPartitionDesc(parent, false);
        List *or_expr_args = NIL;

        // Gather constraints from each non-default partition
        for (int k = 0; k < pdesc->nparts; k++) {
            Oid inhrelid = pdesc->oids[k];
            PartitionBoundSpec *bspec = /* get partition bound from catalog */;

            if (!bspec->is_default) {
                List *part_qual = get_qual_for_range(parent, bspec, true);
                or_expr_args = lappend(or_expr_args,
                    list_length(part_qual) > 1 ? makeBoolExpr(AND_EXPR, part_qual, -1)
                                               : linitial(part_qual));
            }
        }

        // Create NOT expression of all other partition constraints
        if (or_expr_args != NIL) {
            Expr *other_parts_constr = makeBoolExpr(AND_EXPR,
                lappend(get_range_nulltest(key),
                       list_length(or_expr_args) > 1
                           ? makeBoolExpr(OR_EXPR, or_expr_args, -1)
                           : linitial(or_expr_args)), -1);

            result = list_make1(makeBoolExpr(NOT_EXPR,
                                           list_make1(other_parts_constr), -1));
        }
        return result;
    }

    // Add NOT NULL tests for all partition key columns
    if (!for_default)
        result = get_range_nulltest(key);

    // Process each pair of lower/upper bounds
    i = 0;
    forboth(cell1, spec->lowerdatums, cell2, spec->upperdatums) {
        PartitionRangeDatum *ldatum = lfirst_node(PartitionRangeDatum, cell1);
        PartitionRangeDatum *udatum = lfirst_node(PartitionRangeDatum, cell2);

        get_range_key_properties(key, i, ldatum, udatum,
                                &partexprs_item, &keyCol, &lower_val, &upper_val);

        // Test if lower and upper bounds are equal
        if (lower_val && upper_val) {
            // Create and evaluate equality test
            Expr *test_expr = make_partition_op_expr(key, i, BTEqualStrategyNumber,
                                                    (Expr *) lower_val, (Expr *) upper_val);
            bool bounds_equal = /* evaluate test_expr */;

            if (!bounds_equal)
                break;  // Different bounds - need OR expressions

            // Equal bounds - create simple equality constraint
            result = lappend(result, make_partition_op_expr(key, i, BTEqualStrategyNumber,
                                                          keyCol, (Expr *) lower_val));
        } else {
            break;  // NULL bounds (MINVALUE/MAXVALUE) - need OR expressions
        }
        i++;
    }

    // Generate OR expressions for remaining columns with different bounds
    List *lower_or_arms = NIL, *upper_or_arms = NIL;

    // Build lower bound OR arms: (a > al OR (a = al AND b > bl) OR ...)
    if (/* need lower bound constraints */) {
        // Generate cascading OR expressions for lower bounds
        lower_or_arms = /* build lower bound OR expressions */;
    }

    // Build upper bound OR arms: (a < au OR (a = au AND b < bu) OR ...)
    if (/* need upper bound constraints */) {
        // Generate cascading OR expressions for upper bounds
        upper_or_arms = /* build upper bound OR expressions */;
    }

    // Add OR expressions to result
    if (lower_or_arms != NIL)
        result = lappend(result, list_length(lower_or_arms) > 1
                               ? makeBoolExpr(OR_EXPR, lower_or_arms, -1)
                               : linitial(lower_or_arms));

    if (upper_or_arms != NIL)
        result = lappend(result, list_length(upper_or_arms) > 1
                               ? makeBoolExpr(OR_EXPR, upper_or_arms, -1)
                               : linitial(upper_or_arms));

    // Return result or TRUE constant if no constraints
    if (result == NIL)
        result = for_default ? get_range_nulltest(key)
                            : list_make1(makeBoolConst(true, false));

    return result;
}
```