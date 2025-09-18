# create_valuesscan_plan

## Location
src/backend/optimizer/plan/createplan.c: 3847 - 3890

## Overview
Creates a ValuesScan plan node for scanning a VALUES clause base relation with the specified target list and scan clauses.

## Definition


## Detailed Description
This function creates a ValuesScan plan node for executing a VALUES clause scan. It processes the VALUES lists from the range table entry, handles restriction clauses by sorting them for optimal execution order, and manages nestloop parameter substitution when the path has parameter information. The function extracts the VALUES lists from the corresponding range table entry and ensures proper integration with the rest of the query plan through generic path information copying.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planner state and context information
- `best_path`: Path structure representing the chosen access path for this VALUES scan
- `tlist`: Target list specifying which columns/expressions to return from the scan
- `scan_clauses`: List of restriction clauses (WHERE conditions) to apply during scanning

## Dependencies
- Functions called/Symbols referenced:
  - planner_rt_fetch
  - [order_qual_clauses](../o/order_qual_clauses.md)
  - [extract_actual_clauses](../e/extract_actual_clauses.md)
  - [replace_nestloop_params](../r/replace_nestloop_params.md)
  - [make_valuesscan](../m/make_valuesscan.md)
  - [copy_generic_path_info](copy_generic_path_info.md)
  - ValuesScan (return type)
  - RTE_VALUES (constant)
- Called from (representative examples):
  - [create_scan_plan](create_scan_plan.md)

## Notes and Other Information
- The function assumes the scan_relid corresponds to a VALUES base relation (RTE_VALUES)
- Handles nestloop parameter substitution for both scan clauses and VALUES lists when parameterized paths are involved
- The restriction clauses are optimized by sorting them into the best execution order before being processed
- Pseudoconstant clauses are filtered out during clause extraction to improve execution efficiency