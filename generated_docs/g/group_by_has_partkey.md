# group_by_has_partkey

## Location
src/backend/optimizer/plan/planner.c: 8084 - 8170

## Overview
Determines whether all partition keys of a given relation are included in the GROUP BY clause with matching collation, enabling partitionwise aggregation optimization.

## Definition


## Detailed Description
This function checks if partitionwise aggregation can be performed by verifying that all partition key expressions are present in the GROUP BY clause. For partitionwise aggregation to be effective, every partition key must be included in the grouping operation to ensure that all rows of any given group come from the same partition.

The function performs a comprehensive comparison between partition key expressions and GROUP BY expressions, handling RelabelType nodes that may wrap expressions due to type coercions. It also enforces collation matching requirements - if both the partition key and grouping expression have valid collations, they must match exactly.

The algorithm iterates through each partition key attribute, then through all alternative expressions for that key (since a partition key can have multiple equivalent expressions), attempting to find a matching expression in the GROUP BY clause.

## Parameters / Member Variables
- : RelOptInfo for the partitioned input relation being examined
- : List of target list entries containing the expressions available for grouping
- : List of SortGroupClause nodes representing the GROUP BY clause

## Dependencies
- Functions called/Symbols referenced:
  - [get_sortgrouplist_exprs](get_sortgrouplist_exprs.md)
  - [exprCollation](../e/exprCollation.md)
  - [equal](../e/equal.md)
  - RelabelType (type checking)
- Called from (representative examples):
  - [create_ordinary_grouping_paths](../c/create_ordinary_grouping_paths.md)

## Notes and Other Information
- The function requires the input relation to be partitioned (asserts on part_scheme)
- Returns false immediately if no partition key expressions exist
- Handles RelabelType nodes by unwrapping them to access the underlying expression
- Enforces strict collation matching when both partition and group expressions have valid collations
- All partition keys must be matched for the function to return true - even one missing key causes failure
- This check is essential for determining whether full partitionwise aggregation is possible versus requiring partial partitionwise aggregation