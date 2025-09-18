# ProjectSet

## Location
src/include/nodes/plannodes.h: 208 - 211

## Overview
ProjectSet is a plan node that applies projection operations involving set-returning functions (SRFs) to output tuples from an outer plan, handling the expansion of single input rows into multiple output rows.

## Definition


## Detailed Description
The ProjectSet node is specifically designed to handle set-returning functions in the SELECT clause of queries. When a query includes functions that return multiple rows or sets (such as unnest(), generate_series(), or table-valued functions), PostgreSQL uses a ProjectSet node to manage the expansion of each input tuple into potentially multiple output tuples.

Unlike regular projection operations that maintain a 1:1 relationship between input and output rows, ProjectSet handles the complex case where a single input row can generate multiple output rows. This node coordinates the execution of multiple set-returning functions, ensuring proper synchronization when multiple SRFs are used in the same SELECT clause.

The node relies on the common Plan structure for basic plan information, with the actual set-returning function logic handled by the executor through specialized expression evaluation mechanisms.

## Parameters / Member Variables
- : Base Plan structure containing common fields including targetlist (which contains the set-returning function expressions), costs, and tree structure

## Dependencies
- Functions called/Symbols referenced:
  - Plan (base structure)

- Called from (representative examples):
  - make_project_set (optimizer/plan/createplan.c:7013)
  - create_project_set_plan (optimizer/plan/createplan.c:1615)
  - ExecInitProjectSet (executor/nodeProjectSet.c:227)
  - create_group_result_plan (optimizer/plan/createplan.c:1612)

## Notes and Other Information
- ProjectSet nodes are created when the planner detects set-returning functions in the targetlist
- Handles complex scenarios where multiple SRFs need to be coordinated in the same SELECT clause
- The executor implementation manages the stateful nature of set-returning functions across multiple calls
- Critical for supporting PostgreSQL's advanced SQL features like lateral joins with set-returning functions
- Different from regular Result nodes because it must handle the potential for multiple output rows per input row
- The actual SRF evaluation logic is encapsulated in the expression evaluation system, not in the ProjectSet structure itself
- Commonly appears in queries using functions like unnest(), json_array_elements(), generate_series(), and user-defined table functions