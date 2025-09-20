# deconstruct_recurse

## Location
[src/backend/optimizer/plan/initsplan.c:822-1119](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/initsplan.c#L822-L1119)

## Overview
Recursively traverses the query's join tree to extract join structure information and build hierarchical join lists while handling different join types and domain assignments.

## Definition

```c
static List *
deconstruct_recurse(PlannerInfo *root, Node *jtnode,
					JoinDomain *parent_domain,
					JoinTreeItem *parent_jtitem,
					List **item_list)
```
## Detailed Description
This function performs the core recursive traversal of PostgreSQL's join tree structure, processing different node types and building the necessary data structures for join planning. It handles three main types of join tree nodes:

**RangeTblRef nodes**: Base relations that are added to all_baserels and assigned to the parent domain with simple qualscope setup.

**FromExpr nodes**: Represent implicit inner joins from comma-separated table lists. The function recursively processes all child nodes and makes intelligent decisions about collapsing subproblems based on from_collapse_limit to balance planning efficiency with plan quality.

**JoinExpr nodes**: Handle explicit joins (INNER, LEFT, SEMI, ANTI, FULL) with sophisticated domain management:
- INNER/SEMI joins use the parent domain
- LEFT/ANTI joins create new child domains for proper qual placement
- FULL joins require separate domains for each side plus their own domain

The function creates JoinTreeItem structures that track essential information including qualscope (relations involved), join domains, and nonnullable_rels for outer join semantics.

## Parameters / Member Variables
- : The PlannerInfo structure containing global planning context
- : The current join tree node being processed (RangeTblRef, FromExpr, or JoinExpr)
- : The enclosing join domain for proper qual assignment
- : The parent JoinTreeItem in the hierarchy, NULL at top level
- : In/out parameter collecting JoinTreeItem structures in depth-first order

## Dependencies
- Functions called/Symbols referenced:
  - palloc0_object
  - [bms_add_member](../b/bms_add_member.md)
  - [bms_make_singleton](../b/bms_make_singleton.md)
  - [bms_add_members](../b/bms_add_members.md)
  - [bms_union](../b/bms_union.md)
  - [bms_copy](../b/bms_copy.md)
  - [list_concat](../l/list_concat.md)
  - list_make1
  - list_make2
  - llast
  - [mark_rels_nulled_by_join](../m/mark_rels_nulled_by_join.md)
  - makeNode
  - nodeTag
- Called from (representative examples):
  - [deconstruct_jointree](deconstruct_jointree.md)
  - [deconstruct_recurse](deconstruct_recurse.md) (recursive calls)

## Notes and Other Information
- Creates JoinTreeItem for each node to track structural information needed later
- Manages join domain hierarchy critical for proper qualification clause placement
- Implements join collapse logic based on from_collapse_limit and join_collapse_limit for optimization
- Handles special cases like FULL JOIN that require forced join ordering
- Tracks outer_join_rels and calls mark_rels_nulled_by_join for proper null semantics
- Different join types have varying domain assignment strategies to ensure correct qual evaluation
- The returned joinlist guides subsequent join ordering decisions in make_one_rel()
- Eliminates JOIN_RIGHT during earlier processing, handling only normalized join types