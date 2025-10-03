# CreateOneShotCachedPlan

## Location
[src/backend/utils/cache/plancache.c:276-365](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/plancache.c#L276-L365)

## Overview
Creates a specialized CachedPlanSource structure intended for single-use execution, optimized for performance by avoiding data copying and using the caller's memory context.

## Definition
```c
CachedPlanSource *CreateOneShotCachedPlan(RawStmt *raw_parse_tree, const char *query_string, CommandTag commandTag)
```

## Detailed Description
CreateOneShotCachedPlan is a performance-optimized variant of CreateCachedPlan designed for queries that will be executed only once. Unlike regular cached plans, one-shot plans avoid expensive data copying operations by directly referencing the caller's data structures. The CachedPlanSource itself is created in the caller's memory context, typically resulting in automatic cleanup when the context is destroyed.

This approach is particularly beneficial for ad-hoc queries or utility commands where the overhead of plan caching and data copying would outweigh the benefits. However, one-shot plans come with significant restrictions: they cannot be saved, copied, or used across transaction boundaries, and they lack invalidation support, requiring completion within the current transaction.

The function sets the is_oneshot flag to true, which signals to other parts of the plan cache system to handle this plan differently from regular cached plans.

## Parameters / Member Variables
- `raw_parse_tree`: The output of raw_parser(), or NULL for empty queries (not copied, directly referenced)
- `query_string`: The original SQL query text (not copied, directly referenced)  
- `commandTag`: The command tag identifying the type of SQL statement, or NULL for empty queries

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md)
  - CACHEDPLANSOURCE_MAGIC (for structure validation)
- Called from (representative examples):
  - [_SPI_prepare_oneshot_plan](../S/_SPI_prepare_oneshot_plan.md) (SPI interface for one-shot execution)

## Notes and Other Information
- Performance optimization: no data copying occurs, all references point to caller's memory
- Memory context is set to CurrentMemoryContext (caller's context) rather than a dedicated context
- Cannot be saved (SaveCachedPlan will reject one-shot plans)
- Cannot be copied or reused across transactions
- No invalidation support - must complete execution in current transaction  
- DDL operations that could invalidate the plan must be avoided during execution
- The is_oneshot flag is set to true to distinguish from regular cached plans
- Primarily used by SPI for utility commands and ad-hoc query execution
- Trade-off between performance (no copying) and functionality (single-use only)

## Simplified Source

```c
CachedPlanSource *CreateOneShotCachedPlan(RawStmt *raw_parse_tree,
                                          const char *query_string,
                                          CommandTag commandTag) {

    // Create plan source in caller's memory context (no copying)
    CachedPlanSource *plansource = palloc0(sizeof(CachedPlanSource));

    // Set basic identification and structure validation
    plansource->magic = CACHEDPLANSOURCE_MAGIC;
    plansource->raw_parse_tree = raw_parse_tree;  // Direct reference, no copy
    plansource->query_string = query_string;      // Direct reference, no copy
    plansource->commandTag = commandTag;

    // Initialize parameters and options (typically not used for one-shot)
    plansource->param_types = NULL;
    plansource->num_params = 0;
    plansource->parserSetup = NULL;
    plansource->parserSetupArg = NULL;
    plansource->cursor_options = 0;

    // Initialize result handling
    plansource->fixed_result = false;
    plansource->resultDesc = NULL;

    // Memory context management
    plansource->context = CurrentMemoryContext;  // Caller's context
    plansource->query_context = NULL;

    // Initialize lists and dependencies (empty for one-shot)
    plansource->query_list = NIL;
    plansource->relationOids = NIL;
    plansource->invalItems = NIL;

    // Security and row-level security
    plansource->search_path = NULL;
    plansource->rewriteRoleId = InvalidOid;
    plansource->rewriteRowSecurity = false;
    plansource->dependsOnRLS = false;

    // Plan state and statistics
    plansource->gplan = NULL;
    plansource->generation = 0;
    plansource->generic_cost = -1;
    plansource->total_custom_cost = 0;
    plansource->num_generic_plans = 0;
    plansource->num_custom_plans = 0;

    // One-shot specific flags
    plansource->is_oneshot = true;       // Key marker for one-shot behavior
    plansource->is_complete = false;
    plansource->is_saved = false;
    plansource->is_valid = false;

    return plansource;
}
```