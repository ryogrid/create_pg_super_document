# CreateCachedPlan

## Location
[src/backend/utils/cache/plancache.c:192-275](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/plancache.c#L192-L275)

## Overview
Creates and initializes a new CachedPlanSource structure, which serves as the foundation for PostgreSQL's plan caching system.

## Definition
```c
CachedPlanSource *CreateCachedPlan(RawStmt *raw_parse_tree, const char *query_string, CommandTag commandTag)
```

## Detailed Description
CreateCachedPlan is the first phase of a two-step process for creating cached execution plans in PostgreSQL. It creates a CachedPlanSource structure that contains the raw parse tree and other metadata needed for plan creation. The function is designed to be called after raw parsing but before parse analysis and rewrite to optimize memory usage by avoiding unnecessary copying of the parse tree.

The function creates a dedicated memory context for the CachedPlanSource to ensure proper memory management and cleanup in case of errors. Initially, this context is a child of the caller's context, allowing automatic cleanup on error. The cached plan can later be made longer-lived using SaveCachedPlan.

The created CachedPlanSource is initialized with default values for most fields, with the actual planning and optimization occurring later during CompleteCachedPlan or when the plan is first executed.

## Parameters / Member Variables
- `raw_parse_tree`: The output of raw_parser(), or NULL for empty queries  
- `query_string`: The original SQL query text (required as of PostgreSQL 8.4)
- `commandTag`: The command tag identifying the type of SQL statement, or UNKNOWN for empty queries

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [palloc0](../p/palloc0.md)
  - copyObject
  - [pstrdup](../p/pstrdup.md)
  - [MemoryContextSetIdentifier](../M/MemoryContextSetIdentifier.md)
- Called from (representative examples):
  - [PrepareQuery](../P/PrepareQuery.md) (for PREPARE statements)
  - [_SPI_prepare_plan](../S/_SPI_prepare_plan.md) (SPI interface)
  - [exec_parse_message](../e/exec_parse_message.md) (protocol message handling)

## Notes and Other Information
- Part of a two-phase plan creation process (CreateCachedPlan followed by CompleteCachedPlan)
- Creates a dedicated memory context named "CachedPlanSource" for proper resource management
- Initializes the magic number CACHEDPLANSOURCE_MAGIC for structure validation
- All string and tree data is deep-copied to ensure independence from caller's memory
- The is_complete flag is set to false until CompleteCachedPlan is called
- Memory context identifier is set to the query string for debugging purposes
- Critical for PostgreSQL's plan caching infrastructure used in prepared statements, SPI, and protocol-level statement preparation

## Simplified Source

```c
// Simplified version of CreateCachedPlan
CachedPlanSource *CreateCachedPlan(RawStmt *raw_parse_tree,
                                   const char *query_string,
                                   CommandTag commandTag) {
    CachedPlanSource *plansource;
    MemoryContext source_context;
    MemoryContext oldcxt;

    // Validate required parameter
    Assert(query_string != NULL);

    // Step 1: Create dedicated memory context for this cached plan
    source_context = AllocSetContextCreate(CurrentMemoryContext,
                                           "CachedPlanSource",
                                           ALLOCSET_START_SMALL_SIZES);

    // Step 2: Switch to new context and allocate the main structure
    oldcxt = MemoryContextSwitchTo(source_context);

    plansource = (CachedPlanSource *) palloc0(sizeof(CachedPlanSource));

    // Step 3: Initialize core identification fields
    plansource->magic = CACHEDPLANSOURCE_MAGIC;
    plansource->raw_parse_tree = copyObject(raw_parse_tree);
    plansource->query_string = pstrdup(query_string);
    plansource->commandTag = commandTag;
    plansource->context = source_context;

    // Set memory context identifier for debugging
    MemoryContextSetIdentifier(source_context, plansource->query_string);

    // Step 4: Initialize all other fields to default values
    // Parameter-related fields
    plansource->param_types = NULL;
    plansource->num_params = 0;
    plansource->parserSetup = NULL;
    plansource->parserSetupArg = NULL;

    // Execution-related fields
    plansource->cursor_options = 0;
    plansource->fixed_result = false;
    plansource->resultDesc = NULL;

    // Plan and dependency tracking fields
    plansource->query_list = NIL;
    plansource->relationOids = NIL;
    plansource->invalItems = NIL;
    plansource->search_path = NULL;
    plansource->query_context = NULL;
    plansource->gplan = NULL;

    // Security and permission fields
    plansource->rewriteRoleId = InvalidOid;
    plansource->rewriteRowSecurity = false;
    plansource->dependsOnRLS = false;

    // State tracking flags
    plansource->is_oneshot = false;
    plansource->is_complete = false;
    plansource->is_saved = false;
    plansource->is_valid = false;

    // Cost tracking and statistics
    plansource->generation = 0;
    plansource->generic_cost = -1;
    plansource->total_custom_cost = 0;
    plansource->num_generic_plans = 0;
    plansource->num_custom_plans = 0;

    // Step 5: Restore original memory context and return
    MemoryContextSwitchTo(oldcxt);

    return plansource;
}
```

Key simplifications made:
- Grouped field initialization by logical categories with descriptive comments
- Added step-by-step comments to show the main flow
- Removed verbose inline comments while preserving essential information
- Organized the extensive field initialization into logical groups
- Maintained all original functionality and field assignments
- Simplified the memory context creation explanation