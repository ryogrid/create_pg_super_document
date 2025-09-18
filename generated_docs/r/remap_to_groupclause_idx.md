# remap_to_groupclause_idx

## Location
src/backend/optimizer/plan/planner.c: 2258 - 2294

## Overview
Converts grouping sets from sort group reference identifiers to zero-based column indices within a specific groupClause ordering.

## Definition
```c
static List *remap_to_groupclause_idx(List *groupClause, List *gsets, int *tleref_to_colnum_map)
```

## Detailed Description
This utility function performs the crucial transformation of grouping set specifications from their original form using sort group references (tleSortGroupRef) to a form using zero-based indices into a specific groupClause. This transformation is necessary because:

1. **Original Form**: Grouping sets initially reference columns using sort group reference numbers (tleSortGroupRef values)
2. **Index Form**: The executor needs grouping sets as lists of zero-based column indices into the groupClause
3. **Order Independence**: Different rollups may have different groupClause orderings, requiring separate remapping

The function works in two phases:
1. **Mapping Creation**: Builds a mapping table from sort group references to column indices by iterating through the groupClause
2. **Set Transformation**: Converts each grouping set by looking up each sort group reference in the mapping table

This transformation is essential for the executor to correctly identify which columns belong to each grouping set.

## Parameters / Member Variables
- `groupClause`: List of SortGroupClause items defining the column ordering and properties
- `gsets`: List of GroupingSetData items containing grouping sets to be remapped
- `tleref_to_colnum_map`: Workspace array for mapping sort group references to column numbers (modified by this function)

## Dependencies
- Functions called/Symbols referenced:
  - lfirst_node (SortGroupClause, GroupingSetData)
  - lappend_int, lfirst_int
- Called from (representative examples):
  - preprocess_grouping_sets
  - consider_groupingsets_paths

## Notes and Other Information
- Located in src/backend/optimizer/plan/planner.c:2258-2294
- This is a static utility function used during grouping sets preprocessing
- The function modifies the tleref_to_colnum_map array as a side effect
- Returns a new list of lists (grouping sets) with integer column indices instead of sort group references
- Essential for bridging the gap between parse tree representation and execution plan requirements
- Used multiple times during planning: once for regular rollups and once for hash-only sets