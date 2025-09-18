# pull_up_union_leaf_queries

## Location
src/backend/optimizer/prep/prepjointree.c: 1551 - 1620

## Overview
A recursive function that builds AppendRelInfo structures for each leaf query in a setop (set operation) tree and applies query pull-up optimization to those leaf queries.

## Definition


## Detailed Description
This function is the recursive core of the UNION ALL optimization process. It traverses a set operation tree structure, identifying leaf queries (represented by RangeTblRef nodes) and intermediate set operations (SetOperationStmt nodes). For each leaf query found, it:

1. Creates an AppendRelInfo structure that establishes the parent-child relationship between the outer query and the leaf query
2. Builds a translation list using make_setop_translation_list to map columns between parent and child
3. Recursively applies pull_up_subqueries to optimize the leaf query further

The function handles two types of nodes:
- **RangeTblRef**: A leaf query that gets processed with AppendRelInfo creation and recursive optimization
- **SetOperationStmt**: An intermediate set operation node that requires further traversal to reach leaf queries

This is part of PostgreSQL's query optimization strategy for UNION ALL operations, allowing the optimizer to treat multiple queries as append relations rather than separate subqueries.

## Parameters / Member Variables
- : The current node in the setop tree being processed (either RangeTblRef for leaves or SetOperationStmt for internal nodes)
- : PlannerInfo containing the overall query planning context and structures
- : Index of the append relation parent in the root query's range table
- : The Query node containing the setOp tree, whose target list references all setop output columns
- : Offset indicating where child RTEs were copied in the parent's range table (0 when called from flatten_simple_union_all)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (for creating AppendRelInfo and RangeTblRef)
  - make_setop_translation_list
  - lappend 
  - pull_up_subqueries_recurse
  - IsA (macro for type checking)
  - nodeTag
  - elog (for error reporting)
- Called from (representative examples):
  - pull_up_simple_union_all
  - flatten_simple_union_all
  - pull_up_union_leaf_queries (recursive calls)

## Notes and Other Information
- The function is static, meaning it's only used within the prepjointree.c compilation unit
- It handles the recursive nature of set operation trees by calling itself on left and right arguments of SetOperationStmt nodes
- The AppendRelInfo structure must be built before calling pull_up_subqueries_recurse because that function may modify it
- The function can safely pass NULL for containing-join info even under outer joins because child expressions don't propagate up to the join level
- Error handling includes checking for unrecognized node types with appropriate error reporting