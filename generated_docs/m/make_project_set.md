# make_project_set

## Location
[src/backend/optimizer/plan/createplan.c:7010-7028](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L7010-L7028)

## Overview
Creates and initializes a ProjectSet plan node, which is used to handle set-returning functions (SRFs) that return multiple rows from a single input row.

## Definition

```c
static ProjectSet *
make_project_set(List *tlist,
				 Plan *subplan)
```
## Detailed Description
The  function constructs a ProjectSet plan node used in PostgreSQL's query execution to handle set-returning functions. A ProjectSet node is responsible for executing functions that can return multiple rows for each input row, such as unnest() or generate_series(). The function initializes the basic Plan structure within the ProjectSet node, setting up the target list and connecting it to its subplan in the execution tree.

## Parameters / Member Variables
- : Target list (List *) - The list of target entries that define what columns/expressions this node should output
- : Subplan (Plan *) - The child plan node that provides input rows to this ProjectSet node

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create the ProjectSet node)
  - [ProjectSet](../P/ProjectSet.md) (the plan node type being created)
- Called from (representative examples):
  - [create_project_set_plan](../c/create_project_set_plan.md) (primary caller that builds ProjectSet plans from ProjectSetPath)

## Notes and Other Information
- This is a static helper function used internally within the query planner
- The function sets the qual (qualification/WHERE conditions) to NIL since ProjectSet nodes don't filter rows
- Only sets lefttree to the subplan; righttree is always NULL for ProjectSet nodes
- Part of PostgreSQL's plan creation infrastructure in the optimizer module
- [ProjectSet](../P/ProjectSet.md) nodes are specifically designed to handle the complexities of set-returning functions in the target list

## Simplified Source

```c
// Simplified version of make_project_set
static ProjectSet *make_project_set(List *tlist, Plan *subplan) {
    // Create new ProjectSet node
    ProjectSet *node = makeNode(ProjectSet);
    Plan *plan = &node->plan;

    // Set up plan structure
    plan->targetlist = tlist;      // Target list with SRFs
    plan->qual = NIL;              // No filtering
    plan->lefttree = subplan;      // Input plan
    plan->righttree = NULL;        // ProjectSet is unary operator

    return node;
}
```

Key simplifications made:
- Removed detailed comments for clarity
- Focused on the core logic: create node, set target list, connect subplan
- Preserved essential functionality for handling set-returning functions
- Maintained the simple structure of the original function