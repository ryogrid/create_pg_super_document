# deconstruct_jointree

## Location
[src/backend/optimizer/plan/initsplan.c:740-821](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/initsplan.c#L740-L821)

## Overview
Recursively processes the query's join tree to extract and organize WHERE and JOIN/ON clauses, creating a joinlist structure for join order planning.

## Definition

```c
List *
deconstruct_jointree(PlannerInfo *root)
```
## Detailed Description
This function serves as the main entry point for analyzing and deconstructing a query's join tree structure. It performs a comprehensive scan of the jointree to extract qualification clauses and organize them appropriately for the query planner. The function operates in multiple phases:

1. **Preparation phase**: Freezes PlaceHolderInfo creation and initializes the top-level join domain
2. **Recursive scanning**: Calls deconstruct_recurse to traverse the entire join tree, extracting clauses and building join structure
3. **Clause distribution**: Distributes extracted clauses to appropriate RelOptInfo nodes using deconstruct_distribute
4. **Special join handling**: Processes any postponed LEFT JOIN clauses if outer joins are present

The function returns a "joinlist" - a hierarchical structure that guides make_one_rel() in determining valid join orders. Sub-joinlists may be created for FULL OUTER JOINs or when join collapse limits are reached, representing subproblems to be planned separately.

## Parameters / Member Variables
- `*root`: The PlannerInfo structure containing query tree and planning context
## Dependencies
- Functions called/Symbols referenced:
  - linitial_node
  - [deconstruct_recurse](deconstruct_recurse.md)
  - [bms_union](../b/bms_union.md)
  - [bms_equal](../b/bms_equal.md)
  - [deconstruct_distribute](deconstruct_distribute.md)
  - [deconstruct_distribute_oj_quals](deconstruct_distribute_oj_quals.md)
  - [list_free_deep](../l/list_free_deep.md)
- Called from (representative examples):
  - [query_planner](../q/query_planner.md)

## Notes and Other Information
- Sets root->placeholdersFrozen = true to prevent further PlaceHolderInfo creation during join tree processing
- Initializes root->all_baserels and root->outer_join_rels which are populated during recursive scanning
- Creates root->all_query_rels as the union of base relations and outer join relations
- The returned joinlist structure constrains join ordering decisions based on SQL semantics and optimizer limits
- Handles special case processing for postponed LEFT JOIN clauses when outer joins are present
- Manages JoinTreeItem structures temporarily during processing but cleans them up before returning
- Critical for establishing proper join order constraints that respect SQL outer join semantics

## Simplified Source

```c
List *deconstruct_jointree(PlannerInfo *root)
{
    List *result;
    JoinDomain *top_jdomain;
    List *item_list = NIL;

    // Freeze PlaceHolderInfo creation - no more can be made after this point
    root->placeholdersFrozen = true;

    // Get the top-level join domain for the query
    top_jdomain = linitial_node(JoinDomain, root->join_domains);
    top_jdomain->jd_relids = NULL;  // filled during recursive processing

    // Validate jointree structure
    Assert(root->parse->jointree != NULL && IsA(root->parse->jointree, FromExpr));

    // Initialize relation sets that will be filled during scanning
    root->all_baserels = NULL;
    root->outer_join_rels = NULL;

    // Phase 1: Recursively scan the jointree to extract clauses and structure
    result = deconstruct_recurse(root, (Node *) root->parse->jointree,
                                top_jdomain, NULL, &item_list);

    // Complete the all_query_rels set
    root->all_query_rels = bms_union(root->all_baserels, root->outer_join_rels);

    // Verify consistency with the top join domain
    Assert(bms_equal(root->all_query_rels, top_jdomain->jd_relids));

    // Phase 2: Distribute extracted clauses to appropriate relations
    foreach(lc, item_list) {
        JoinTreeItem *jtitem = (JoinTreeItem *) lfirst(lc);
        deconstruct_distribute(root, jtitem);
    }

    // Phase 3: Handle postponed LEFT JOIN clauses if any outer joins exist
    if (root->join_info_list) {
        foreach(lc, item_list) {
            JoinTreeItem *jtitem = (JoinTreeItem *) lfirst(lc);

            if (jtitem->oj_joinclauses != NIL)
                deconstruct_distribute_oj_quals(root, item_list, jtitem);
        }
    }

    // Clean up temporary JoinTreeItem structures
    list_free_deep(item_list);

    return result;
}
```