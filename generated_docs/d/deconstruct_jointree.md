# deconstruct_jointree

## Location
src/backend/optimizer/plan/initsplan.c: 740 - 821

## Overview
Recursively processes the query's join tree to extract and organize WHERE and JOIN/ON clauses, creating a joinlist structure for join order planning.

## Definition


## Detailed Description
This function serves as the main entry point for analyzing and deconstructing a query's join tree structure. It performs a comprehensive scan of the jointree to extract qualification clauses and organize them appropriately for the query planner. The function operates in multiple phases:

1. **Preparation phase**: Freezes PlaceHolderInfo creation and initializes the top-level join domain
2. **Recursive scanning**: Calls deconstruct_recurse to traverse the entire join tree, extracting clauses and building join structure
3. **Clause distribution**: Distributes extracted clauses to appropriate RelOptInfo nodes using deconstruct_distribute
4. **Special join handling**: Processes any postponed LEFT JOIN clauses if outer joins are present

The function returns a "joinlist" - a hierarchical structure that guides make_one_rel() in determining valid join orders. Sub-joinlists may be created for FULL OUTER JOINs or when join collapse limits are reached, representing subproblems to be planned separately.

## Parameters / Member Variables
- : The PlannerInfo structure containing query tree and planning context

## Dependencies
- Functions called/Symbols referenced:
  - linitial_node
  - deconstruct_recurse
  - bms_union
  - bms_equal
  - deconstruct_distribute
  - deconstruct_distribute_oj_quals
  - list_free_deep
- Called from (representative examples):
  - query_planner

## Notes and Other Information
- Sets root->placeholdersFrozen = true to prevent further PlaceHolderInfo creation during join tree processing
- Initializes root->all_baserels and root->outer_join_rels which are populated during recursive scanning
- Creates root->all_query_rels as the union of base relations and outer join relations
- The returned joinlist structure constrains join ordering decisions based on SQL semantics and optimizer limits
- Handles special case processing for postponed LEFT JOIN clauses when outer joins are present
- Manages JoinTreeItem structures temporarily during processing but cleans them up before returning
- Critical for establishing proper join order constraints that respect SQL outer join semantics