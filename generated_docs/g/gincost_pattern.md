# gincost_pattern

## Location
src/backend/utils/adt/selfuncs.c: 7369 - 7482

## Overview
Estimates the number of index terms that need to be searched for a GIN query pattern and updates the provided cost counts accordingly.

## Definition
static bool gincost_pattern(IndexOptInfo *index, int indexcol, Oid clause_op, Datum query, GinQualCounts *counts)

## Detailed Description
The gincost_pattern function analyzes a specific GIN (Generalized Inverted Index) query pattern to estimate search costs. It calls the index's extractQuery support function to determine how many index entries need to be examined for the given query value. The function distinguishes between exact matches, partial matches, and different search modes (default, include empty, or full scan). It updates the provided GinQualCounts structure with entry counts and search mode information that will be used by the overall GIN cost estimation process. The function returns false if the query is unsatisfiable (no matches possible).

## Parameters / Member Variables
- `index`: IndexOptInfo structure containing information about the GIN index
- `indexcol`: Column number within the index being queried
- `clause_op`: OID of the operator used in the query clause
- `query`: The actual query value (constant) being searched for
- `counts`: GinQualCounts structure to be updated with cost estimation data

## Dependencies
- Functions called/Symbols referenced:
  - get_op_opfamily_properties
  - get_opfamily_proc
  - get_rel_name
  - fmgr_info
  - set_fn_opclass_options
  - FunctionCall7Coll
  - GIN_EXTRACTQUERY_PROC
  - GIN_SEARCH_MODE_DEFAULT
  - GIN_SEARCH_MODE_INCLUDE_EMPTY
- Called from (representative examples):
  - gincost_opexpr
  - gincost_scalararrayopexpr

## Notes and Other Information
- Uses a heuristic estimate of 100 matched entries for partial matches
- Handles three search modes: DEFAULT (normal index scan), INCLUDE_EMPTY (treats empty entries as matches), and ALL (full index scan)
- Calls the index's extractQuery support function to get actual search terms
- Updates both exact and partial entry counts for cost estimation
- Returns false for unsatisfiable queries (when nentries <= 0 in default search mode)
- Properly handles collation settings for the extractQuery function call