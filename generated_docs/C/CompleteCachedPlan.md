# CompleteCachedPlan

## Location
src/backend/utils/cache/plancache.c: 366 - 481

## Overview
CompleteCachedPlan is the second step in creating a plan cache entry that finalizes a CachedPlanSource with the analyzed-and-rewritten query forms and all required subsidiary data.

## Definition


## Detailed Description
CompleteCachedPlan takes an incomplete CachedPlanSource (created by CreateCachedPlan) and populates it with the analyzed-and-rewritten query trees and all necessary metadata. This function handles memory context management for the query trees, extracts query dependencies for cache invalidation, and sets up parameter specifications. After completion, the CachedPlanSource can be used with GetCachedPlan to obtain execution plans and optionally saved with SaveCachedPlan.

The function provides flexible memory management options: it can adopt an existing querytree_context (space-for-time tradeoff) or create a fresh context and copy the query trees. For oneshot plans, it skips copying entirely for performance.

## Parameters / Member Variables
- : The CachedPlanSource structure returned by CreateCachedPlan to be completed
- : List of Query nodes representing the analyzed-and-rewritten form of the query
- : Memory context containing querytree_list, or NULL to copy into a fresh context  
- : Array of fixed parameter type OIDs, or NULL if no parameters
- : Number of fixed parameters in the query
- : Alternate method for handling query parameters (hook function)
- : Data to pass to the parserSetup hook function
- : Options bitmask to pass to the planner for cursor-related behavior
- : True to disallow future changes in the query's result tuple descriptor

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextSetParent
  - AllocSetContextCreate
  - copyObject
  - StmtPlanRequiresRevalidation
  - extract_query_dependencies
  - GetSearchPathMatcher
  - PlanCacheComputeResultDesc
  - CACHEDPLANSOURCE_MAGIC
  - ALLOCSET_START_SMALL_SIZES

- Called from (representative examples):
  - PrepareQuery (src/backend/commands/prepare.c:120)
  - _SPI_prepare_plan (src/backend/executor/spi.c:2287)
  - _SPI_execute_plan (src/backend/executor/spi.c:2540)
  - exec_parse_message (src/backend/tcop/postgres.c:1554)

## Notes and Other Information
- The function marks the CachedPlanSource as complete and valid upon successful completion
- For oneshot plans, query tree copying is skipped entirely for performance reasons
- The function extracts query dependencies for cache invalidation unless dealing with oneshot plans
- Row Level Security (RLS) information is captured and stored for proper security enforcement
- The current search_path is saved to ensure consistent query planning across different sessions
- Memory context reparenting is used to efficiently manage the lifecycle of query trees