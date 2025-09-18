# is_simple_union_all_recurse

## Location
[src/backend/optimizer/prep/prepjointree.c:2100-2142](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L2100-L2142)

## Overview
This function recursively validates that a set operation tree consists only of UNION ALL operations with compatible datatypes across all leaf queries.

## Definition
```c
static bool is_simple_union_all_recurse(Node *setOp, Query *setOpQuery, List *colTypes)
```

## Detailed Description
The is_simple_union_all_recurse function performs the actual recursive validation for determining if a set operation tree qualifies as a simple UNION ALL. It traverses the set operation tree depth-first, ensuring that:

1. All intermediate nodes are SetOperationStmt nodes with SETOP_UNION and the 'all' flag set
2. All leaf nodes (RangeTblRef) have subqueries whose target lists match the expected column datatypes
3. No datatype coercions are required between different branches of the UNION ALL

The function handles two types of nodes:
- RangeTblRef: Represents leaf queries in the set operation tree. These are validated by checking that their target list datatypes match the expected types.
- SetOperationStmt: Represents intermediate set operation nodes. Must be UNION ALL, and both left and right subtrees are recursively validated.

The function includes stack overflow protection since it can recurse deeply on complex set operation trees.

## Parameters / Member Variables
- `setOp`: Node representing either a RangeTblRef (leaf) or SetOperationStmt (intermediate node) in the set operation tree
- `setOpQuery`: The Query structure containing the range table for resolving RangeTblRef nodes
- `colTypes`: List of expected column datatypes that all leaf queries must match

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth
  - RangeTblRef
  - rt_fetch
  - [tlist_same_datatypes](../t/tlist_same_datatypes.md)
  - SetOperationStmt
  - SETOP_UNION
  - nodeTag
  - [is_simple_union_all_recurse](is_simple_union_all_recurse.md) (recursive calls)
- Called from:
  - [is_simple_union_all](is_simple_union_all.md)
  - [is_simple_union_all_recurse](is_simple_union_all_recurse.md) (recursive)
  - [flatten_simple_union_all](../f/flatten_simple_union_all.md)

## Notes and Other Information
- Implements depth-first traversal of the set operation tree
- Only accepts UNION ALL operations - rejects UNION (without ALL), INTERSECT, and EXCEPT
- Uses tlist_same_datatypes to verify datatype compatibility without checking typmods or collations
- Includes stack overflow protection via check_stack_depth() 
- The recursive nature allows handling of arbitrarily complex UNION ALL trees
- Part of PostgreSQL's subquery flattening optimization infrastructure
- Located in src/backend/optimizer/prep/prepjointree.c:2100-2142