# add_nulling_relids_context

## Location
src/backend/rewrite/rewriteManip.c: 49 - 55

## Overview
A context structure used to track relation IDs and sublevel information when adding nulling relation IDs to Vars and PlaceHolderVars during query tree transformation.

## Definition
```c
typedef struct
{
    const Bitmapset *target_relids;
    const Bitmapset *added_relids;
    int             sublevels_up;
} add_nulling_relids_context;
```

## Detailed Description
This structure serves as a context parameter for the mutator functions that modify Vars and PlaceHolderVars to add nulling relation IDs. This is part of PostgreSQL's outer join processing mechanism, where certain relations may produce NULL values due to outer join semantics. The structure maintains the sets of relation IDs that should be targeted for modification and the relation IDs that should be added as nulling relations.

The context is used in conjunction with `add_nulling_relids()` function to traverse expression trees and modify variables that belong to specific relations, adding information about which relations might cause those variables to become NULL due to outer join processing. This is essential for correct NULL value handling in queries with outer joins.

## Parameters / Member Variables
- `target_relids`: A bitmapset of relation IDs that should be targeted for nulling relation addition; if NULL, all level-zero Vars and PlaceHolderVars are modified
- `added_relids`: A bitmapset of relation IDs to be added to the varnullingrels or phnullingrels fields of matching variables
- `sublevels_up`: Tracks the current query level depth during recursive traversal of subqueries and nested expressions

## Dependencies
- Functions called/Symbols referenced: None (pure data structure)
- Called from (representative examples):
  - add_nulling_relids (src/backend/rewrite/rewriteManip.c:1153)
  - add_nulling_relids_mutator (src/backend/rewrite/rewriteManip.c:1166)

## Notes and Other Information
- Part of PostgreSQL's query rewriting infrastructure for handling outer join semantics
- Critical for maintaining correct NULL-value semantics when transforming queries with outer joins
- Works with both Var nodes (representing table columns) and PlaceHolderVar nodes (representing computed expressions)
- The mutator creates copies of nodes when modifications are needed, preserving the original tree structure
- Used in query optimization phases where join order and outer join processing require tracking of potential NULL-generating relations