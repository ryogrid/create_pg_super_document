# fix_scan_expr_context

## Location
[src/backend/optimizer/plan/setrefs.c:62-72](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/setrefs.c#L62-L72)

## Overview
A context structure used during scan expression fixing to provide necessary information for resolving variable references in scan-level expressions.

## Definition


## Detailed Description
The  structure serves as a context container during the expression fixing phase for scan-level operations in PostgreSQL's query planner. This structure is used when the planner needs to adjust variable references and expression nodes to match the final plan structure.

The context provides essential information needed to properly transform expressions, including access to the planner's global state, range table offset adjustments for subqueries or views, and execution frequency estimates for cost-based optimizations.

This structure is typically passed to expression mutator and walker functions that traverse and modify expression trees during the plan finalization process.

## Parameters / Member Variables
- : Pointer to the PlannerInfo structure containing global planner state and information
- : Range table offset adjustment, used when processing subqueries or views that need RT index translation
- : Estimated number of executions for this scan, used for cost calculations and optimization decisions

## Dependencies
- Functions called/Symbols referenced:
  - [PlannerInfo](../P/PlannerInfo.md) (planner's main state structure)
- Called from (representative examples):
  - fix_scan_list
  - [fix_scan_expr](fix_scan_expr.md)
  - [fix_scan_expr_mutator](fix_scan_expr_mutator.md)
  - [fix_scan_expr_walker](fix_scan_expr_walker.md)

## Notes and Other Information
- Used specifically for scan-level expression fixing, distinct from join and upper-level contexts
- The rtoffset field is crucial for correctly translating variable references in nested query contexts
- The num_exec field helps the planner make informed decisions about expression evaluation costs
- Part of the broader expression reference fixing framework in setrefs.c