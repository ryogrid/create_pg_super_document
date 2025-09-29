# create_group_plan

## Location
[src/backend/optimizer/plan/createplan.c:2242-2280](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L2242-L2280)

## Overview
Creates a Group plan node for performing grouping operations, extracting grouping columns and operators from the GroupPath and building the necessary execution structure.

## Definition

```c
static Group *
create_group_plan(PlannerInfo *root, GroupPath *best_path)
```
## Detailed Description
The  function generates a Group plan node from a GroupPath, which is used to implement GROUP BY operations in PostgreSQL queries. Unlike Sort nodes, Group nodes can perform projection, so they are less restrictive about the child's target list but still need access to the grouping columns.

The function builds the target list for the Group node using the path information, processes any qualification clauses, and extracts the grouping information including grouping columns, operators, and collations from the GroupPath's groupClause. This information is essential for the executor to properly group rows based on the specified criteria.

The Group node implementation assumes that input data is already sorted by the grouping columns, which is typically ensured by having a Sort node as a child or by leveraging index ordering.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning information and context
- : GroupPath structure representing the chosen grouping strategy and associated GROUP BY clauses

## Dependencies
- Functions called/Symbols referenced:
  - : Recursively creates execution plans for subpaths with CP_LABEL_TLIST flag
  - : Constructs the target list for the Group plan from path information
  - : Orders qualification clauses for optimal execution
  - : Creates the actual Group plan node with specified parameters
  - : Extracts grouping column numbers from the GROUP BY clause
  - : Extracts equality operators for grouping columns
  - : Extracts collation information for grouping columns
  - : Copies common path information to the plan node
- Called from (representative examples):
  - : Main plan creation dispatch function

## Notes and Other Information
- This is a static function, only accessible within the createplan.c compilation unit
- [Group](../G/Group.md) nodes can perform projection, making them more flexible than Sort nodes in terms of target list requirements
- The CP_LABEL_TLIST flag ensures that grouping columns are properly labeled and available for extraction
- The function relies on three extraction functions to properly set up grouping metadata: columns, operators, and collations
- [Group](../G/Group.md) operations assume pre-sorted input data - the planner ensures this by including appropriate Sort nodes when necessary
- The qualification clauses (HAVING clauses) are processed and ordered for optimal execution performance

## Simplified Source

```c
static Group *
create_group_plan(PlannerInfo *root, GroupPath *best_path)
{
    // Create subplan with grouping columns available
    Plan *subplan = create_plan_recurse(root, best_path->subpath, CP_LABEL_TLIST);

    // Build target list for the Group operation
    List *tlist = build_path_tlist(root, &best_path->path);

    // Process qualification clauses (HAVING clauses)
    List *quals = order_qual_clauses(root, best_path->qual);

    // Create the Group plan node
    Group *plan = make_group(
        tlist,
        quals,
        list_length(best_path->groupClause),
        extract_grouping_cols(best_path->groupClause, subplan->targetlist),
        extract_grouping_ops(best_path->groupClause),
        extract_grouping_collations(best_path->groupClause, subplan->targetlist),
        subplan
    );

    // Copy generic path information
    copy_generic_path_info(&plan->plan, (Path *) best_path);

    return plan;
}
```