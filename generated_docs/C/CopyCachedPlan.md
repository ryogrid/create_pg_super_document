# CopyCachedPlan

## Location
[src/backend/utils/cache/plancache.c:1536-1626](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/plancache.c#L1536-L1626)

## Overview
Creates a complete deep copy of a CachedPlanSource, producing an unsaved, complete cached plan with all data structures duplicated in new memory contexts.

## Definition
```c
CachedPlanSource *CopyCachedPlan(CachedPlanSource *plansource)
```

## Detailed Description
CopyCachedPlan performs a comprehensive duplication of a CachedPlanSource, creating an independent copy with its own memory contexts and data structures. This function is equivalent to manually calling CreateCachedPlan followed by CompleteCachedPlan using the source plan's data.

The copying process includes:
- Creating new memory contexts for the plan source and query trees
- Deep copying the raw parse tree, query list, and all associated metadata
- Duplicating parameter information, relation OIDs, and invalidation items
- Copying search path, security settings, and cost estimation data
- Preserving validity state and generation information

The resulting copy is always marked as unsaved (regardless of the source's state) and does not include any generic plan that may exist in the source. The copy inherits the same validity state as the original.

## Parameters / Member Variables
- `plansource`: The source CachedPlanSource to copy (must be complete and not one-shot)

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [MemoryContextSetIdentifier](../M/MemoryContextSetIdentifier.md)
  - copyObject
  - [CreateTupleDescCopy](CreateTupleDescCopy.md)
  - [CopySearchPathMatcher](CopySearchPathMatcher.md)
  - CACHEDPLANSOURCE_MAGIC
  - ALLOCSET_START_SMALL_SIZES
- Called from (representative examples):
  - [_SPI_save_plan](../S/_SPI_save_plan.md)

## Notes and Other Information
- Cannot copy one-shot plans since parsing/planning may have modified the raw parse tree or query trees
- The copy is always created as unsaved, complete, and non-oneshot regardless of the source state
- No generic plan is copied - the new plan source will need to generate its own if needed
- Creates separate memory contexts for the plan source and query tree data to maintain proper memory management
- Preserves cost estimation data from the source, which can be valuable for planning decisions
- Critical for SPI operations that need to create persistent copies of temporary plans
- The copy maintains all security and access control information from the original
- Used primarily when converting temporary plans to saved plans in the SPI interface

## Simplified Source

```c
CachedPlanSource *
CopyCachedPlan(CachedPlanSource *plansource)
{
    // Validate input and check constraints
    Assert(plansource->magic == CACHEDPLANSOURCE_MAGIC);
    Assert(plansource->is_complete);

    // One-shot plans cannot be copied safely
    if (plansource->is_oneshot)
        elog(ERROR, "cannot copy a one-shot cached plan");

    // Create new memory context for the copied plan source
    MemoryContext source_context = AllocSetContextCreate(CurrentMemoryContext,
                                                          "CachedPlanSource",
                                                          ALLOCSET_START_SMALL_SIZES);

    MemoryContext oldcxt = MemoryContextSwitchTo(source_context);

    // Allocate and initialize new plan source
    CachedPlanSource *newsource = (CachedPlanSource *) palloc0(sizeof(CachedPlanSource));
    newsource->magic = CACHEDPLANSOURCE_MAGIC;

    // Copy basic plan data
    newsource->raw_parse_tree = copyObject(plansource->raw_parse_tree);
    newsource->query_string = pstrdup(plansource->query_string);
    MemoryContextSetIdentifier(source_context, newsource->query_string);
    newsource->commandTag = plansource->commandTag;

    // Copy parameter information
    if (plansource->num_params > 0)
    {
        newsource->param_types = (Oid *) palloc(plansource->num_params * sizeof(Oid));
        memcpy(newsource->param_types, plansource->param_types,
               plansource->num_params * sizeof(Oid));
    }
    else
        newsource->param_types = NULL;
    newsource->num_params = plansource->num_params;

    // Copy parser and cursor options
    newsource->parserSetup = plansource->parserSetup;
    newsource->parserSetupArg = plansource->parserSetupArg;
    newsource->cursor_options = plansource->cursor_options;
    newsource->fixed_result = plansource->fixed_result;

    // Copy result descriptor if present
    if (plansource->resultDesc)
        newsource->resultDesc = CreateTupleDescCopy(plansource->resultDesc);
    else
        newsource->resultDesc = NULL;

    newsource->context = source_context;

    // Create separate context for query tree data
    MemoryContext querytree_context = AllocSetContextCreate(source_context,
                                                             "CachedPlanQuery",
                                                             ALLOCSET_START_SMALL_SIZES);
    MemoryContextSwitchTo(querytree_context);

    // Copy query-related data
    newsource->query_list = copyObject(plansource->query_list);
    newsource->relationOids = copyObject(plansource->relationOids);
    newsource->invalItems = copyObject(plansource->invalItems);
    if (plansource->search_path)
        newsource->search_path = CopySearchPathMatcher(plansource->search_path);
    newsource->query_context = querytree_context;

    // Copy security and RLS settings
    newsource->rewriteRoleId = plansource->rewriteRoleId;
    newsource->rewriteRowSecurity = plansource->rewriteRowSecurity;
    newsource->dependsOnRLS = plansource->dependsOnRLS;

    // Initialize plan state (no generic plan copied)
    newsource->gplan = NULL;
    newsource->is_oneshot = false;
    newsource->is_complete = true;
    newsource->is_saved = false;  // Always unsaved
    newsource->is_valid = plansource->is_valid;
    newsource->generation = plansource->generation;

    // Copy cost estimation data
    newsource->generic_cost = plansource->generic_cost;
    newsource->total_custom_cost = plansource->total_custom_cost;
    newsource->num_generic_plans = plansource->num_generic_plans;
    newsource->num_custom_plans = plansource->num_custom_plans;

    MemoryContextSwitchTo(oldcxt);

    return newsource;
}
```