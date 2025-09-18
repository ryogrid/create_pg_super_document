# make_sort

## Location
src/backend/optimizer/plan/createplan.c: 6069 - 6098

## Overview
The make_sort function is a basic utility routine that creates a Sort plan node in PostgreSQL's query planner, setting up the necessary sorting specifications and linking it to its child plan.

## Definition


## Detailed Description
The make_sort function constructs a Sort plan node that represents a sorting operation in PostgreSQL's execution plan tree. It initializes the Sort node with the provided sorting specifications and connects it to its child plan (lefttree). The function assumes that the caller has already prepared all the sorting-related arrays (sortColIdx, sortOperators, collations, and nullsFirst) with the appropriate values.

The created Sort node inherits the target list from its child plan and has no additional qualification conditions. It serves as a fundamental building block for implementing various sorting operations in query execution plans.

## Parameters / Member Variables
- : The child plan node that provides the input tuples to be sorted
- : The number of columns to sort by
- : Array of column indices (attribute numbers) to sort by
- : Array of OIDs for the sorting operators to use for each column
- : Array of OIDs for the collation rules to apply to each sorting column
- : Array of boolean flags indicating whether NULLs should sort before non-NULLs for each column

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create the Sort node)
  - Sort (plan node type)
- Called from (representative examples):
  - create_append_plan (src/backend/optimizer/plan/createplan.c:1360)
  - create_merge_append_plan (src/backend/optimizer/plan/createplan.c:1532)
  - make_sort_from_pathkeys (src/backend/optimizer/plan/createplan.c:6367)
  - make_sort_from_sortclauses (src/backend/optimizer/plan/createplan.c:6446)
  - make_sort_from_groupcols (src/backend/optimizer/plan/createplan.c:6500)

## Notes and Other Information
- This is a static function within createplan.c, used internally by the planner
- The function performs basic initialization and does not validate the sorting parameters
- The caller is responsible for ensuring that all sorting arrays are properly constructed and have the correct length (numCols)
- The Sort node created will have no right child (righttree = NULL) and no additional qualifications (qual = NIL)
- Located at src/backend/optimizer/plan/createplan.c:6069-6098