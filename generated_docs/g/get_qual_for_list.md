# get_qual_for_list

## Location
src/backend/partitioning/partbounds.c: 4066 - 4274

## Overview
Generates a CHECK constraint expression for a list partition by creating equality comparisons with allowed values and handling NULL values appropriately.

## Definition
static List *get_qual_for_list(Relation parent, PartitionBoundSpec *spec)

## Detailed Description
This function constructs the partition constraint for a list partition, which validates that the partition key equals one of the allowed values specified in the partition definition. The function handles both regular list partitions (with explicit value lists) and default list partitions (which accept values not in any other partition).

For regular list partitions, it creates equality expressions comparing the partition key to each allowed value. For default partitions, it generates a constraint that excludes all values used by other partitions. The function also properly handles NULL values, creating appropriate IS NULL or IS NOT NULL tests based on whether the partition accepts nulls.

The constraint generation includes special logic for single-column list partitioning and ensures proper null handling based on the partition specification.

## Parameters / Member Variables
- parent: The parent relation that is being partitioned
- spec: Partition bound specification containing the list of allowed values and default partition flag

## Dependencies
- Functions called/Symbols referenced:
  - [RelationGetPartitionKey](../R/RelationGetPartitionKey.md)
  - makeVar
  - copyObject
  - [RelationGetPartitionDesc](../R/RelationGetPartitionDesc.md)
  - partition_bound_accepts_nulls
  - [makeConst](../m/makeConst.md)
  - [datumCopy](../d/datumCopy.md)
  - [make_partition_op_expr](../m/make_partition_op_expr.md)
  - makeNode
  - [makeBoolExpr](../m/makeBoolExpr.md)
  - [make_ands_explicit](../m/make_ands_explicit.md)
- Called from (representative examples):
  - [get_qual_from_partbound](get_qual_from_partbound.md)
  - [check_default_partition_contents](../c/check_default_partition_contents.md)

## Notes and Other Information
- Only supports single-column list partitioning (key->partnatts == 1)
- For default partitions that are the only partition, returns NIL (no constraint needed)
- Handles both attribute-based and expression-based partition keys
- Creates IS NOT NULL tests for partitions that dont accept nulls
- Creates IS NULL tests for partitions that do accept nulls
- For default partitions, applies NOT to the entire constraint expression to exclude other partition values
- The generated constraints never evaluate to NULL, making NOT application work as intended