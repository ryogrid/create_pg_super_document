# get_qual_for_range

## Location
src/backend/partitioning/partbounds.c: 4275 - 4631

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
  - ExecEvalExprSwitchContext
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