# make_project_set

## Location
src/backend/optimizer/plan/createplan.c: 7010 - 7028

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
  - ProjectSet (the plan node type being created)
- Called from (representative examples):
  - create_project_set_plan (primary caller that builds ProjectSet plans from ProjectSetPath)

## Notes and Other Information
- This is a static helper function used internally within the query planner
- The function sets the qual (qualification/WHERE conditions) to NIL since ProjectSet nodes don't filter rows
- Only sets lefttree to the subplan; righttree is always NULL for ProjectSet nodes
- Part of PostgreSQL's plan creation infrastructure in the optimizer module
- ProjectSet nodes are specifically designed to handle the complexities of set-returning functions in the target list