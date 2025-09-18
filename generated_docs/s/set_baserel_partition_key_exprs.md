# set_baserel_partition_key_exprs

## Location
[src/backend/optimizer/util/plancat.c:2556-2623](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/plancat.c#L2556-L2623)

## Overview
Builds partition key expressions for a base relation and populates the rel->partexprs field with expressions that represent the partition key columns.

## Definition


## Detailed Description
This function constructs partition key expressions for a partitioned base relation by processing the partition key definition from the relation's metadata. For each partition key attribute, it creates either a Var node (for simple column references) or copies and re-stamps complex expressions with the correct relation number. The function ensures that partition key expressions are properly formatted for use in query planning and optimization, particularly for partition pruning and join optimization.

The function handles two types of partition key expressions:
1. Simple column references (stored as Var nodes with the relation's varno)
2. Complex expressions (copied from the partition key definition and re-stamped with the correct varno)

It also initializes the nullable_partexprs array as empty lists since base relations don't have nullable partition expressions (no outer joins involved).

## Parameters / Member Variables
- : The Relation structure representing the partitioned table
- : The RelOptInfo structure to populate with partition key expressions

## Dependencies
- Functions called/Symbols referenced:
  - [RelationGetPartitionKey](../R/RelationGetPartitionKey.md)
  - IS_SIMPLE_REL
  - list_head
  - makeVar
  - copyObject
  - [ChangeVarNodes](../C/ChangeVarNodes.md)
  - [lnext](../l/lnext.md)
- Called from (representative examples):
  - [set_relation_partition_info](set_relation_partition_info.md)

## Notes and Other Information
- This is a static function used internally within plancat.c for partition-related query planning
- The function assumes the relation is a simple base relation (IS_SIMPLE_REL) with a valid relid
- Base relations have exactly one expression per partition key, unlike join relations which may have multiple
- The nullable_partexprs field is allocated but left empty for base relations since they don't participate in outer joins
- Error checking ensures the number of partition key expressions matches the expected count