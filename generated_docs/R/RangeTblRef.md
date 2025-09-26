# RangeTblRef

## Location
[src/include/nodes/primnodes.h:2243-2247](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L2243-L2247)

## Overview
RangeTblRef is a simple node structure that references an entry in a query's range table through an index, serving as a leaf node in the join tree structure.

## Definition

```c
typedef struct RangeTblRef
{
	NodeTag		type;
	int			rtindex;
} RangeTblRef;
```
## Detailed Description
RangeTblRef serves as a reference mechanism to entries in the query's range table (RT). Instead of using direct pointers to RT entries, PostgreSQL uses these index-based references to avoid the complexities and headaches that arise from having multiple pointers to the same node in a query tree structure.

RangeTblRef nodes function as the leaves of a join tree structure. Above these leaf nodes, JoinExpr nodes can appear to denote specific kinds of joins or qualified joins, and FromExpr nodes can appear to denote ordinary cross-product joins. The design choice to use indices rather than direct pointers provides better structural integrity and easier manipulation of the query tree.

During the parsing process, the raw output of gram.y contains RangeVar, RangeSubselect, and RangeFunction nodes, which are all eventually replaced by RangeTblRef nodes during the parse analysis phase.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a RangeTblRef node
- `rtindex`: Integer index pointing to the corresponding entry in the query's range table
## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (for node identification)
- Called from (representative examples):
  - [make_rel_from_joinlist](../m/make_rel_from_joinlist.md) (path/allpaths.c:3333)
  - [add_base_rels_to_query](../a/add_base_rels_to_query.md) (plan/initsplan.c:161)
  - [transformFromClauseItem](../t/transformFromClauseItem.md) (parse_clause.c:1067)
  - [get_from_clause_item](../g/get_from_clause_item.md) (ruleutils.c:12039)
  - [pull_up_subqueries_recurse](../p/pull_up_subqueries_recurse.md) (prepjointree.c:988)
  - [markQueryForLocking](../m/markQueryForLocking.md) (rewriteHandler.c:1887)

## Notes and Other Information
- [RangeTblRef](RangeTblRef.md) nodes are the fundamental building blocks for representing table references in PostgreSQL's internal query representation
- The use of indices instead of pointers is a deliberate design decision to prevent issues with shared references in complex query trees
- These nodes are created during parse analysis, replacing the original grammar-level constructs
- They work in conjunction with JoinExpr and FromExpr nodes to form complete join tree structures
- The rtindex field must be a valid index into the query's range table for the node to be meaningful