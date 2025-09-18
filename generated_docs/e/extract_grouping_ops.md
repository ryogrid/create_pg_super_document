# extract_grouping_ops

## Location
src/backend/optimizer/util/tlist.c: 463 - 488

## Overview
Extracts equality operator OIDs from a SortGroupClause list and returns them as an array for use in planning GROUP BY operations.

## Definition


## Detailed Description
This utility function processes a list of SortGroupClause structures and extracts the equality operator OIDs (eqop field) from each clause. It creates and returns a dynamically allocated array containing these operator OIDs in the same order as they appear in the input list. The function is primarily used during query planning to prepare operator information needed for grouping operations.

The function allocates memory for the result array using palloc() and iterates through the input list, copying each SortGroupClause's equality operator OID to the corresponding position in the output array. It includes an assertion to ensure that each extracted operator OID is valid.

## Parameters / Member Variables
- : A List of SortGroupClause structures from which to extract equality operator OIDs

## Dependencies
- Functions called/Symbols referenced:
  - list_length (to determine array size)
  - palloc (for memory allocation)
  - lfirst (for list iteration)
  - OidIsValid (for assertion checking)
  - SortGroupClause (structure type)
- Called from (representative examples):
  - create_group_plan (src/backend/optimizer/plan/createplan.c:2264)
  - create_agg_plan (src/backend/optimizer/plan/createplan.c:2332)
  - create_groupingsets_plan (src/backend/optimizer/plan/createplan.c:2490, 2529)

## Notes and Other Information
- The returned array is allocated with palloc() and becomes the caller's responsibility to manage
- The function assumes all SortGroupClause entries have valid equality operator OIDs
- Used in conjunction with other grouping extraction functions to prepare complete operator and collation information for query execution plans
- Located in src/backend/optimizer/util/tlist.c:463-488