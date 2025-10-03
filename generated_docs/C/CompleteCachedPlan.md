# CompleteCachedPlan

## Location
[src/backend/utils/cache/plancache.c:366-481](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/plancache.c#L366-L481)

## Overview
CompleteCachedPlan is the second step in creating a plan cache entry that finalizes a CachedPlanSource with the analyzed-and-rewritten query forms and all required subsidiary data.

## Definition

```c
void
CompleteCachedPlan(CachedPlanSource *plansource,
				   List *querytree_list,
				   MemoryContext querytree_context,
				   Oid *param_types,
				   int num_params,
				   ParserSetupHook parserSetup,
				   void *parserSetupArg,
				   int cursor_options,
				   bool fixed_result)
```
## Detailed Description
CompleteCachedPlan takes an incomplete CachedPlanSource (created by CreateCachedPlan) and populates it with the analyzed-and-rewritten query trees and all necessary metadata. This function handles memory context management for the query trees, extracts query dependencies for cache invalidation, and sets up parameter specifications. After completion, the CachedPlanSource can be used with GetCachedPlan to obtain execution plans and optionally saved with SaveCachedPlan.

The function provides flexible memory management options: it can adopt an existing querytree_context (space-for-time tradeoff) or create a fresh context and copy the query trees. For oneshot plans, it skips copying entirely for performance.

## Parameters / Member Variables
- `*plansource`: The CachedPlanSource structure returned by CreateCachedPlan to be completed
- `*querytree_list`: List of Query nodes representing the analyzed-and-rewritten form of the query
- `querytree_context`: Memory context containing querytree_list, or NULL to copy into a fresh context
- `*param_types`: Array of fixed parameter type OIDs, or NULL if no parameters
- `num_params`: Number of fixed parameters in the query
- `parserSetup`: Alternate method for handling query parameters (hook function)
- `*parserSetupArg`: Data to pass to the parserSetup hook function
- `cursor_options`: Options bitmask to pass to the planner for cursor-related behavior
- `fixed_result`: True to disallow future changes in the query's result tuple descriptor
## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextSetParent](../M/MemoryContextSetParent.md)
  - AllocSetContextCreate
  - copyObject
  - StmtPlanRequiresRevalidation
  - [extract_query_dependencies](../e/extract_query_dependencies.md)
  - [GetSearchPathMatcher](../G/GetSearchPathMatcher.md)
  - [PlanCacheComputeResultDesc](../P/PlanCacheComputeResultDesc.md)
  - CACHEDPLANSOURCE_MAGIC
  - ALLOCSET_START_SMALL_SIZES

- Called from (representative examples):
  - [PrepareQuery](../P/PrepareQuery.md) (src/backend/commands/prepare.c:120)
  - [_SPI_prepare_plan](../S/_SPI_prepare_plan.md) (src/backend/executor/spi.c:2287)
  - [_SPI_execute_plan](../S/_SPI_execute_plan.md) (src/backend/executor/spi.c:2540)
  - [exec_parse_message](../e/exec_parse_message.md) (src/backend/tcop/postgres.c:1554)

## Notes and Other Information
- The function marks the CachedPlanSource as complete and valid upon successful completion
- For oneshot plans, query tree copying is skipped entirely for performance reasons
- The function extracts query dependencies for cache invalidation unless dealing with oneshot plans
- Row Level Security (RLS) information is captured and stored for proper security enforcement
- The current search_path is saved to ensure consistent query planning across different sessions
- Memory context reparenting is used to efficiently manage the lifecycle of query trees

## Simplified Source

```c
// Simplified version of CompleteCachedPlan
void CompleteCachedPlan(CachedPlanSource *plansource,
                       List *querytree_list,
                       MemoryContext querytree_context,
                       Oid *param_types,
                       int num_params,
                       ParserSetupHook parserSetup,
                       void *parserSetupArg,
                       int cursor_options,
                       bool fixed_result)
{
    MemoryContext source_context = plansource->context;
    MemoryContext oldcxt = CurrentMemoryContext;

    // Validate that the plan source is in the correct state
    Assert(plansource->magic == CACHEDPLANSOURCE_MAGIC);
    Assert(!plansource->is_complete);

    // Handle memory context setup for query trees
    if (plansource->is_oneshot) {
        // Oneshot plans: use current context, no copying needed
        querytree_context = CurrentMemoryContext;
    }
    else if (querytree_context != NULL) {
        // Reuse provided context: reparent under source context
        MemoryContextSetParent(querytree_context, source_context);
        MemoryContextSwitchTo(querytree_context);
    }
    else {
        // Create fresh context and copy query trees
        querytree_context = AllocSetContextCreate(source_context,
                                                  "CachedPlanQuery",
                                                  ALLOCSET_START_SMALL_SIZES);
        MemoryContextSwitchTo(querytree_context);
        querytree_list = copyObject(querytree_list);
    }

    // Store query context and query list in plan source
    plansource->query_context = querytree_context;
    plansource->query_list = querytree_list;

    // Extract dependencies for cache invalidation (skip for oneshot plans)
    if (!plansource->is_oneshot && StmtPlanRequiresRevalidation(plansource)) {
        // Extract relation dependencies and invalidation items
        extract_query_dependencies((Node *) querytree_list,
                                   &plansource->relationOids,
                                   &plansource->invalItems,
                                   &plansource->dependsOnRLS);

        // Capture RLS info for security enforcement
        plansource->rewriteRoleId = GetUserId();
        plansource->rewriteRowSecurity = row_security;

        // Save current search_path for consistent planning
        plansource->search_path = GetSearchPathMatcher(querytree_context);
    }

    // Store parameter types and other configuration in source context
    MemoryContextSwitchTo(source_context);

    if (num_params > 0) {
        // Copy parameter type array
        plansource->param_types = (Oid *) palloc(num_params * sizeof(Oid));
        memcpy(plansource->param_types, param_types, num_params * sizeof(Oid));
    }
    else {
        plansource->param_types = NULL;
    }

    // Store all configuration parameters
    plansource->num_params = num_params;
    plansource->parserSetup = parserSetup;
    plansource->parserSetupArg = parserSetupArg;
    plansource->cursor_options = cursor_options;
    plansource->fixed_result = fixed_result;

    // Compute and store result tuple descriptor
    plansource->resultDesc = PlanCacheComputeResultDesc(querytree_list);

    // Restore original memory context
    MemoryContextSwitchTo(oldcxt);

    // Mark plan source as complete and valid
    plansource->is_complete = true;
    plansource->is_valid = true;
}
```

Key simplifications made:
- Removed detailed comments about memory management trade-offs for clarity
- Consolidated parameter copying logic into a clearer if-else structure
- Abstracted low-level memory operations with high-level comments
- Focused on the main execution path and core functionality
- Added descriptive comments for each major logical step
- Simplified the dependency extraction section while preserving essential logic