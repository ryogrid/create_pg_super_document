# create_foreignscan_plan

## Location
src/backend/optimizer/plan/createplan.c: 4122 - 4276

## Overview
Creates a ForeignScan plan node for scanning a relation using a Foreign Data Wrapper (FDW), handling both base relations and foreign joins with appropriate optimization and parameter handling.

## Definition


## Detailed Description
This function is responsible for creating a ForeignScan execution plan node from a ForeignPath. It coordinates with the Foreign Data Wrapper (FDW) to generate an optimized plan for accessing foreign data. The function handles multiple scenarios including base foreign table scans, foreign joins, and parameterized foreign scans. It ensures proper cost propagation, handles outer plan creation for complex foreign operations, and manages system column detection for base relations. The function also performs nestloop parameter replacement for parameterized scans and sets up relid information needed for proper execution planning.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning context and state information
- : ForeignPath representing the chosen access path for the foreign relation
- : Target list specifying which columns/expressions should be returned by the scan
- : List of restriction clauses to be applied during the scan

## Dependencies
- Functions called/Symbols referenced:
  - [create_plan_recurse](create_plan_recurse.md)
  - planner_rt_fetch
  - [order_qual_clauses](../o/order_qual_clauses.md)
  - [copy_generic_path_info](copy_generic_path_info.md)
  - [bms_difference](../b/bms_difference.md)
  - [replace_nestloop_params](../r/replace_nestloop_params.md)
  - [pull_varattnos](../p/pull_varattnos.md)
  - [bms_is_member](../b/bms_is_member.md)
  - [bms_free](../b/bms_free.md)
- Called from (representative examples):
  - [create_scan_plan](create_scan_plan.md)

## Notes and Other Information
- The function delegates actual plan generation to the FDW's GetForeignPlan callback, allowing FDWs to customize their execution strategy
- Handles both simple foreign table scans and complex foreign joins
- Automatically detects and flags when system columns are requested from base relations
- Performs parameter replacement for nested loop joins involving foreign scans
- Sets up relid tracking for proper join planning and execution
- Manages user access permissions and role dependencies for foreign joins
- Located at src/backend/optimizer/plan/createplan.c:4122-4276