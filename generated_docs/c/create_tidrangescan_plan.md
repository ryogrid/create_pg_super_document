# create_tidrangescan_plan

## Location
src/backend/optimizer/plan/createplan.c: 3637 - 3701

## Overview
Creates a TID range scan plan for a base relation that scans a range of tuple identifiers (TIDs) rather than individual TIDs, optimized for range-based TID queries.

## Definition


## Detailed Description
The  function constructs a TidRangeScan execution plan node for scanning ranges of tuple identifiers. Unlike regular TID scans that target specific individual TIDs, this function handles queries that specify TID ranges, allowing for efficient scanning of consecutive rows in a table.

The function is simpler than  because TID range qualifications use AND semantics rather than OR semantics, making duplicate elimination straightforward. It processes the tidrangequals list to filter out redundant scan clauses and prepares the final scan plan.

Key processing steps include:
- Filtering scan clauses to remove duplicates found in tidrangequals (using simple AND semantics)
- Converting RestrictInfo structures to bare expressions
- Replacing outer relation variables with nestloop parameters when needed
- Creating the final TidRangeScan plan with optimized execution order

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and context information
- : TidRangePath representing the chosen access path with TID range qualifications
- : Target list specifying which columns to return from the scan
- : List of restriction clauses to apply during scanning

## Dependencies
- Functions called/Symbols referenced:
  - list_member_ptr
  - order_qual_clauses
  - extract_actual_clauses
  - replace_nestloop_params
  - make_tidrangescan
  - copy_generic_path_info
- Called from (representative examples):
  - create_scan_plan

## Notes and Other Information
- Only works with base relations (RTE_RELATION), similar to regular TID scans
- Uses AND semantics for tidrangequals, making duplicate elimination simpler than TID scans
- More efficient than individual TID scans when scanning consecutive or nearly consecutive rows
- Supports parameterized plans through nestloop parameter replacement
- Particularly useful for queries with TID range conditions like 
- The scan can efficiently process ranges of TIDs without needing to specify each individual TID