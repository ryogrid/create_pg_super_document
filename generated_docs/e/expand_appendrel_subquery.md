# expand_appendrel_subquery

## Location
[src/backend/optimizer/util/inherit.c:799-841](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/inherit.c#L799-L841)

## Overview
Creates RelOptInfo structures for child relations of an appendrel baserel that represents a flattened UNION ALL subquery.

## Definition

```c
static void
expand_appendrel_subquery(PlannerInfo *root, RelOptInfo *rel,
						  RangeTblEntry *rte, Index rti)
```
## Detailed Description
This function handles the expansion of subquery relations that have been flattened into appendrel structures. When a UNION ALL subquery is flattened during query planning, the parent subquery becomes an appendrel baserel, and its component subqueries become child relations listed in root->append_rel_list. This function creates the necessary RelOptInfo structures for each child subquery so that the planner can generate execution plans for them.

The function iterates through all append relations in the planner's append_rel_list, identifies those that belong to the current parent relation, and builds RelOptInfo structures for each child. If a child relation is itself an inherited relation (either a table inheritance or another subquery), the function recursively expands it as well.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing planner state including append_rel_list
- `*rel`: RelOptInfo for the parent subquery relation (the flattened UNION ALL)
- `*rte`: RangeTblEntry for the parent subquery relation
- `rti`: Range table index of the parent relation
## Dependencies
- Functions called/Symbols referenced:
  - [build_simple_rel](../b/build_simple_rel.md) (creates RelOptInfo for child relations)
  - [expand_inherited_rtentry](expand_inherited_rtentry.md) (recursively handles inherited child relations)
  - [AppendRelInfo](../A/AppendRelInfo.md) (data structure for append relation information)
- Called from (representative examples):
  - [expand_inherited_rtentry](expand_inherited_rtentry.md)

## Notes and Other Information
- Only processes append relations that match the current parent relation ID (rti)
- Uses Assert statements to verify child RTE exists and is properly indexed
- Handles nested inheritance where child relations may themselves be inherited
- Part of PostgreSQL's subquery flattening and UNION ALL optimization system  
- The rte->inh flag indicates this is an inheritable relation (flattened UNION ALL)
- Located in src/backend/optimizer/util/inherit.c at lines 799-841

## Simplified Source

```c
static void expand_appendrel_subquery(PlannerInfo *root, RelOptInfo *rel,
                                      RangeTblEntry *rte, Index rti) {
    ListCell *l;

    // Process all append relations for this parent
    foreach(l, root->append_rel_list) {
        AppendRelInfo *appinfo = (AppendRelInfo *) lfirst(l);
        Index childRTindex = appinfo->child_relid;
        RangeTblEntry *childrte;
        RelOptInfo *childrel;

        // Skip append rels for other parents
        if (appinfo->parent_relid != rti)
            continue;

        // Get the child RTE
        childrte = root->simple_rte_array[childRTindex];

        // Build RelOptInfo for the child subquery
        childrel = build_simple_rel(root, childRTindex, rel);

        // Recursively handle inherited children
        if (childrte->inh)
            expand_inherited_rtentry(root, childrel, childrte, childRTindex);
    }
}
```