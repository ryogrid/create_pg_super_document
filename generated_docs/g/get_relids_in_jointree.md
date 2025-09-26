# get_relids_in_jointree

## Location
[src/backend/optimizer/prep/prepjointree.c:4081-4141](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L4081-L4141)

## Overview
Recursively traverses a join tree structure to extract the set of range table (RT) indexes present within it, with control over whether to include outer joins and inner joins.

## Definition

```c
Relids
get_relids_in_jointree(Node *jtnode, bool include_outer_joins,
					   bool include_inner_joins)
```
## Detailed Description
This function performs a recursive traversal of PostgreSQL's join tree structure to collect all relation IDs (relids) present within the tree. The join tree is a hierarchical representation of the FROM clause in SQL queries, containing base relations, subqueries, and various types of joins.

The function handles three main node types:
1. **RangeTblRef**: Base relation references - always included in the result
2. **FromExpr**: Represents a list of relations in the FROM clause - recursively processes each element
3. **JoinExpr**: Represents join operations - processes left and right arguments recursively, with optional inclusion of the join's own RT index based on join type

The inclusion of join RT indexes is controlled by the boolean parameters:
- For inner joins: included only if  is true
- For outer joins: included only if  is true

This selective inclusion is particularly important during subquery flattening where inner join RT indexes may need special handling.

## Parameters / Member Variables
- : The join tree node to process (can be RangeTblRef, FromExpr, or JoinExpr)
- : Whether to include outer-join RT indexes in the result set
- : Whether to include inner-join RT indexes in the result set (typically false except during subquery flattening)

## Dependencies
- Functions called/Symbols referenced:
  - bms_make_singleton
  - bms_join
  - bms_add_member
  - nodeTag
  - get_relids_in_jointree (recursive)
- Called from (representative examples):
  - preprocess_rowmarks
  - pull_up_simple_subquery
  - is_simple_subquery
  - remove_result_refs
  - find_dependent_phvs_in_jointree
  - get_relids_for_join

## Notes and Other Information
- Base-relation relids are always included regardless of the boolean parameters
- For most planner purposes, outer joins are included in standard relid sets
- Setting  to true is only appropriate for special purposes during subquery flattening
- The function uses PostgreSQL's bitmap set (bms) operations for efficient set manipulation
- Returns NULL if the input node is NULL, otherwise returns a Relids bitmap set