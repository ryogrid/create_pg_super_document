# create_foreignscan_path

## Location
src/backend/optimizer/util/pathnode.c: 2235 - 2280

## Overview
Creates a path node for scanning a foreign base table through PostgreSQL's Foreign Data Wrapper (FDW) interface, allowing access to external data sources.

## Definition


## Detailed Description
This function constructs a ForeignPath node specifically for foreign table scan operations. Unlike other path creation functions in PostgreSQL core, this function is never called directly by core PostgreSQL code. Instead, it's designed to be called by Foreign Data Wrapper (FDW) implementations through their GetForeignPaths function. The FDW must supply all cost and row estimation fields since PostgreSQL core has no way to calculate these values for external data sources. The function creates a specialized ForeignPath structure that extends the basic Path structure with FDW-specific fields for storing optimizer state and private data.

## Parameters / Member Variables
- : PlannerInfo structure containing global information about the query being planned
- : RelOptInfo structure representing the foreign table relation being scanned
- : PathTarget specifying the desired output columns and expressions (NULL defaults to rel->reltarget)
- : Estimated number of rows this path will return
- : Estimated cost to begin returning tuples
- : Estimated total cost to return all tuples
- : List of PathKey structures specifying the output ordering
- : Set of relation IDs that must be available as outer relations
- : Optional outer path for join pushdown scenarios
- : List of restriction clauses that can be handled by the FDW
- : FDW-specific private data for storing implementation details

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - IS_SIMPLE_REL
  - get_baserel_parampathinfo
- Called from (representative examples):
  - Foreign Data Wrapper implementations (external to core PostgreSQL)

## Notes and Other Information
- Returns a ForeignPath structure, not a basic Path structure
- Sets pathtype to T_ForeignScan to identify this as a foreign scan path
- Includes an assertion that the relation must be a simple relation (IS_SIMPLE_REL)
- The FDW must provide all cost estimates since core PostgreSQL cannot calculate them
- Supports the pathtarget defaulting to rel->reltarget when target parameter is NULL
- [Path](../P/Path.md) is marked as not parallel-aware but respects the relation's parallel safety settings
- Essential for PostgreSQL's extensibility through the FDW interface
- The fdw_private field allows FDWs to store implementation-specific optimization data