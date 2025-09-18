# create_scan_plan

## Location
src/backend/optimizer/plan/createplan.c: 560 - 825

## Overview
Creates scan plans for relation access by extracting restriction clauses, building target lists, and delegating to specialized scan plan creation functions based on the path type.

## Definition
```c
static Plan *create_scan_plan(PlannerInfo *root, Path *best_path, int flags)
```

## Detailed Description
create_scan_plan serves as the central dispatcher for creating all types of scan plans in PostgreSQL. It handles the common logic for scan plan creation including extracting and processing restriction clauses, handling parameterized scans, determining appropriate target lists, and managing gating clauses for pseudoconstant conditions. The function distinguishes between different scan types and delegates to specialized creation functions while optimizing target list generation through the use of physical target lists when beneficial.

The function handles both base relation scans and join-replacement scans (for ForeignScan and CustomScan), applies different clause extraction strategies for index scans versus other scan types, and manages the addition of gating Result nodes when pseudoconstant clauses are present.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planner state and context information
- `best_path`: The path node representing the chosen scan strategy to be converted into a plan
- `flags`: Control flags affecting target list generation and labeling behavior (CP_IGNORE_TLIST, CP_LABEL_TLIST, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - use_physical_tlist
  - build_physical_tlist
  - build_path_tlist
  - get_gating_quals
  - apply_pathtarget_labeling_to_tlist
  - list_concat_copy
  - copyObject
  - create_seqscan_plan
  - create_samplescan_plan
  - create_indexscan_plan
  - create_bitmap_scan_plan
  - create_tidscan_plan
  - create_tidrangescan_plan
  - create_subqueryscan_plan
  - create_functionscan_plan
  - create_tablefuncscan_plan
  - create_valuesscan_plan
  - create_ctescan_plan
  - create_namedtuplestorescan_plan
  - create_resultscan_plan
  - create_worktablescan_plan
  - create_foreignscan_plan
  - create_customscan_plan
  - create_gating_plan
  - IS_JOIN_REL (macro)
- Called from (representative examples):
  - create_plan_recurse

## Notes and Other Information
- For IndexScan and IndexOnlyScan, uses indrestrictinfo instead of baserestrictinfo to avoid redundant predicate checks
- Handles parameterized scans by adding join clauses from outer relations to the scan clauses
- Optimizes target list generation by preferring physical target lists when possible to enable executor tuple projection optimization
- For IndexOnlyScan, uses the index's target list instead of building a physical one
- Supports both base relations and join-replacement scans (foreign/custom scans)
- Automatically adds gating Result nodes for pseudoconstant qualification evaluation
- Falls back to regular target list building when physical target list generation fails due to dropped columns
- Located at src/backend/optimizer/plan/createplan.c:560-825