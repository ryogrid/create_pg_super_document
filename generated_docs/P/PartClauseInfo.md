# PartClauseInfo

## Location
src/backend/partitioning/partprune.c: 62 - 71

## Overview
PartClauseInfo is a structure that holds information about a clause that has been matched with a partition key, used in PostgreSQL's partition pruning optimization.

## Definition


## Detailed Description
PartClauseInfo is a crucial data structure in PostgreSQL's partition pruning mechanism, defined in src/backend/partitioning/partprune.c:62-71. This structure encapsulates all the necessary information about a WHERE clause condition that can be used to eliminate partitions during query planning. When the query planner encounters conditions that reference partition keys, it creates PartClauseInfo structures to represent these conditions in a form suitable for partition elimination logic.

The structure serves as an intermediate representation that bridges the gap between the original SQL WHERE clause conditions and the partition pruning algorithms. It standardizes how partition-related conditions are represented internally, making it easier for the pruning logic to work with different types of operators and expressions.

## Parameters / Member Variables
- : The zero-based index of the partition key column (0 to partnatts - 1) that this clause references
- : The OID of the operator used to compare the partition key to the expression (e.g., =, <, >, <=, >=)
- : A boolean flag indicating whether the original operator in the clause was the not-equal operator (<>)
- : Pointer to the expression tree representing the value or expression that the partition key is being compared against
- : The OID of the comparison function used to compare the expression to the partition key values
- : The btree strategy number that identifies the type of comparison operation being performed

## Dependencies
- Functions called/Symbols referenced:
  - [Expr](../E/Expr.md) (expression tree structure)
  - Oid (object identifier type)

- Called from (representative examples):
  - [gen_partprune_steps_internal](../g/gen_partprune_steps_internal.md)
  - [gen_prune_steps_from_opexps](../g/gen_prune_steps_from_opexps.md)
  - [match_clause_to_partition_key](../m/match_clause_to_partition_key.md)
  - [get_steps_using_prefix_recurse](../g/get_steps_using_prefix_recurse.md)

## Notes and Other Information
- This structure is central to PostgreSQL's partition pruning optimization, which can significantly improve query performance by eliminating irrelevant partitions from consideration
- The op_is_ne field is particularly important because not-equal conditions require special handling in partition pruning logic
- The cmpfn field stores the comparison function OID, which is essential for performing the actual comparisons during pruning
- The structure is used extensively throughout the partition pruning code path, appearing in functions that generate pruning steps and match clauses to partition keys
- Located in src/backend/partitioning/partprune.c, this is part of PostgreSQL's partitioning subsystem