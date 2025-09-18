# create_resultscan_path

## Location
src/backend/optimizer/util/pathnode.c: 2176 - 2201

## Overview
Creates a path node for scanning an RTE_RESULT relation, which represents a relation that produces a computed result set without scanning any actual table data.

## Definition


## Detailed Description
This function constructs a Path node specifically for result scan operations on RTE_RESULT relations. RTE_RESULT relations are special relation types that generate computed results, such as VALUES clauses, function calls that return sets, or other expressions that produce tabular data without accessing stored tables. The function sets the pathtype to T_Result and initializes all necessary Path structure fields. Like named tuplestore scans, result scans always produce unordered output since the results are computed rather than retrieved from an ordered source.

## Parameters / Member Variables
- : PlannerInfo structure containing global information about the query being planned
- : RelOptInfo structure representing the RTE_RESULT relation being scanned
- : Set of relation IDs that must be available as outer relations for this path

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - get_baserel_parampathinfo
  - cost_resultscan
- Called from (representative examples):
  - set_result_pathlist
  - reparameterize_path

## Notes and Other Information
- Sets pathtype to T_Result to identify this as a result scan path
- Always sets pathkeys to NIL because computed results are inherently unordered
- The path is marked as not parallel-aware but respects the relation's parallel safety settings
- No parallel workers are assigned (parallel_workers = 0)
- Cost calculation is handled by cost_resultscan function
- Used for scanning relations that generate computed results like VALUES clauses or function calls
- This path type is essential for handling non-table data sources in PostgreSQL queries