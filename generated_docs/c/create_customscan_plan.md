# create_customscan_plan

## Location
[src/backend/optimizer/plan/createplan.c:4277-4347](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L4277-L4347)

## Overview
Transforms a CustomPath into a CustomScan plan node by delegating to custom scan providers while handling child plan creation and parameter substitution.

## Definition

```c
static CustomScan *
create_customscan_plan(PlannerInfo *root, CustomPath *best_path,
					   List *tlist, List *scan_clauses)
```
## Detailed Description
This function creates a CustomScan execution plan node from a CustomPath. Custom scans allow extensions and plugins to implement their own scan methods beyond PostgreSQL's built-in scan types. The function first recursively creates plans for any child paths, then delegates the main plan creation to the custom scan provider's PlanCustomPath callback. It handles cost information copying and manages nestloop parameter replacement for parameterized scans. This provides a framework for extensible scan implementations while maintaining consistency with PostgreSQL's planning infrastructure.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning context and state information  
- : CustomPath representing the chosen custom scan access path
- : Target list specifying which columns/expressions should be returned by the scan
- : List of restriction clauses to be applied during the scan

## Dependencies
- Functions called/Symbols referenced:
  - [create_plan_recurse](create_plan_recurse.md)
  - [order_qual_clauses](../o/order_qual_clauses.md)
  - castNode
  - [copy_generic_path_info](copy_generic_path_info.md)
  - [replace_nestloop_params](../r/replace_nestloop_params.md)
- Called from (representative examples):
  - [create_scan_plan](create_scan_plan.md)

## Notes and Other Information
- Enables extensibility by allowing custom scan providers to implement specialized scan logic
- Recursively handles child plans for complex custom scan operations
- The custom scan provider's PlanCustomPath callback does the actual work of creating the plan
- Automatically handles parameter replacement for nested loop scenarios
- Custom scan providers must register their methods including the PlanCustomPath callback
- Located at src/backend/optimizer/plan/createplan.c:4277-4347