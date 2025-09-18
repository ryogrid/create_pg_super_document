# create_tidscan_plan

## Location
src/backend/optimizer/plan/createplan.c: 3540 - 3636

## Overview
Creates a TID scan plan for a base relation using tuple identifier (TID) values to directly access specific table rows, with restriction clauses and a target list.

## Definition


## Detailed Description
The  function constructs a TidScan execution plan node for directly accessing table rows using their tuple identifiers (TIDs). This is an optimization for queries that specify exact row locations through CTID conditions or similar TID-based predicates.

The function handles the complex task of separating TID-specific qualifications (tidquals) from other scan clauses, ensuring that redundant conditions are eliminated while preserving necessary restrictions. It supports both single and multiple TID qualifications, with special handling for OR semantics in multi-TID cases.

Key processing steps include:
- Filtering scan clauses to remove those redundant with TID qualifications
- Handling single vs multiple TID qualification scenarios differently
- Converting RestrictInfo structures to bare expressions
- Replacing outer relation variables with nestloop parameters when needed
- Creating the final TidScan plan with optimized execution order

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and context information
- : TidPath representing the chosen access path with TID qualifications
- : Target list specifying which columns to return from the scan
- : List of restriction clauses to apply during scanning

## Dependencies
- Functions called/Symbols referenced:
  - list_member_ptr
  - is_redundant_derived_clause
  - order_qual_clauses
  - extract_actual_clauses
  - list_difference
  - make_orclause
  - replace_nestloop_params
  - make_tidscan
  - copy_generic_path_info
- Called from (representative examples):
  - create_scan_plan

## Notes and Other Information
- Only works with base relations (RTE_RELATION), not with subqueries or functions
- Implements sophisticated duplicate elimination to avoid redundant qualification checking
- Uses different strategies for single vs multiple TID qualifications due to OR semantics
- Supports parameterized plans through nestloop parameter replacement
- The resulting plan can directly access specific table rows without index lookups
- TID scans are particularly efficient for queries using CTID predicates or similar direct row addressing