# pull_up_sublinks_jointree_recurse

## Location
[src/backend/optimizer/prep/prepjointree.c:480-636](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L480-L636)

## Overview
Recursively processes jointree nodes for pull_up_sublinks, transforming SubLinks into semijoins while collecting relids of contained relations.

## Definition

```c
union(leftrelids,
																   rightrelids),
														 NULL, NULL);
```
## Detailed Description
This is the core recursive function that implements the SubLink pull-up transformation for different types of jointree nodes. It traverses the query's jointree structure and delegates SubLink processing to pull_up_sublinks_qual_recurse for the qualification clauses.

The function handles different jointree node types:

**RangeTblRef**: Simple base case that returns the relid of the referenced relation without modification.

**FromExpr**: Processes each child in the fromlist recursively, then processes the WHERE clause qualifications. The function builds a new FromExpr with the transformed children and calls pull_up_sublinks_qual_recurse to handle SubLinks in the quals.

**JoinExpr**: Creates a copy of the join node and recursively processes both left and right arguments. The handling of the join quals depends on the join type:
- **INNER JOIN**: SubLinks can be pulled up freely since all relations are available
- **LEFT JOIN**: SubLinks can only be pulled up if they reference the nullable (right) side  
- **RIGHT JOIN**: SubLinks can only be pulled up if they reference the nullable (left) side
- **FULL JOIN**: No SubLink pull-up is performed since both sides may be nullable

The function ensures that pulled-up SubLinks are placed correctly in the join tree structure and maintains proper relid tracking for subsequent optimization phases.

## Parameters / Member Variables
- : PlannerInfo structure containing query optimization context
- : The jointree node to process (RangeTblRef, FromExpr, or JoinExpr)
- : Output parameter that receives the set of relation IDs contained in this subtree

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth
  - [pull_up_sublinks_qual_recurse](pull_up_sublinks_qual_recurse.md)
  - [makeFromExpr](../m/makeFromExpr.md)
  - [bms_make_singleton](../b/bms_make_singleton.md), bms_join, bms_union, bms_add_member
  - [palloc](palloc.md), memcpy
  - lappend, lfirst
  - IsA macro
  - elog, nodeTag
  - JOIN_INNER, JOIN_LEFT, JOIN_RIGHT, JOIN_FULL constants
- Called from (representative examples):
  - [pull_up_sublinks](pull_up_sublinks.md) (in src/backend/optimizer/prep/prepjointree.c:459)  
  - [pull_up_sublinks_qual_recurse](pull_up_sublinks_qual_recurse.md) (multiple locations for recursive SubLink processing)
  - Self-recursive calls for processing child nodes

## Notes and Other Information
- Stack overflow protection via check_stack_depth() due to recursive nature
- Creates modified copies of JoinExpr nodes using palloc/memcpy to avoid affecting original tree
- Handles join alias variables correctly by including join rtindex in returned relids
- Does not include pulled-up subquery relids in returned relids since upper levels cannot reference them
- Relies on subsequent optimization steps to flatten and rearrange the resulting join structure
- Critical component of subquery decorrelation and semijoin optimization
- Works in close coordination with pull_up_sublinks_qual_recurse for actual SubLink transformation