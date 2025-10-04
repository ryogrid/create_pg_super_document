# SPI_cursor_open_internal

## Location
[src/backend/executor/spi.c:1577-1793](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L1577-L1793)

## Overview
SPI_cursor_open_internal is the common internal function that implements cursor opening functionality for all SPI cursor open variants. It creates and starts a Portal for executing SELECT queries as cursors within the Server Programming Interface (SPI).

## Definition

```c
static Portal
SPI_cursor_open_internal(const char *name, SPIPlanPtr plan,
						 ParamListInfo paramLI, bool read_only)
```
## Detailed Description
This internal function handles the core logic for opening SPI cursors. It validates that the provided plan is suitable for cursor operations, creates a Portal with the specified name (or generates one automatically), configures cursor options including scroll behavior, handles parameter binding, and starts portal execution with the appropriate snapshot. The function ensures proper memory context management and error handling throughout the process.

The function performs extensive validation including checking that the plan contains only cursor-compatible queries (primarily SELECT statements), handling scroll cursor restrictions (disallowing SELECT FOR UPDATE with SCROLL), and validating read-only requirements when specified.

## Parameters / Member Variables
- `*name`: The name for the cursor portal (NULL or empty string generates a random name)
- `plan`: The prepared SPIPlan containing the query to execute as a cursor
- `paramLI`: Parameter list information for parameterized queries (can be NULL)
- `read_only`: Boolean flag indicating if cursor should be restricted to read-only operations
## Dependencies
- Functions called/Symbols referenced:
  - [SPI_is_cursor_plan](SPI_is_cursor_plan.md) (validates plan is cursor-compatible)
  - [CreateNewPortal](../C/CreateNewPortal.md)/CreatePortal (creates the portal)
  - [GetCachedPlan](../G/GetCachedPlan.md) (retrieves cached execution plan)
  - [PortalDefineQuery](../P/PortalDefineQuery.md) (associates query with portal)
  - [PortalStart](../P/PortalStart.md) (begins portal execution)
  - [GetActiveSnapshot](../G/GetActiveSnapshot.md)/GetTransactionSnapshot (manages snapshots)
  - [_SPI_begin_call](_SPI_begin_call.md)/_SPI_end_call (SPI stack management)
- Called from (representative examples):
  - [SPI_cursor_open](SPI_cursor_open.md)
  - [SPI_cursor_open_with_args](SPI_cursor_open_with_args.md)
  - [SPI_cursor_open_with_paramlist](SPI_cursor_open_with_paramlist.md)
  - [SPI_cursor_parse_open](SPI_cursor_parse_open.md)

## Notes and Other Information
- This is a static function internal to spi.c and not part of the public SPI API
- Handles both saved and unsaved plans differently for memory management
- Automatically determines scroll behavior based on plan characteristics when not explicitly specified
- Enforces restriction that scrollable cursors must be read-only when using SELECT FOR UPDATE/SHARE
- Manages memory contexts carefully to prevent leaks, especially during error conditions
- Returns a Portal handle that can be used with other SPI cursor functions

## Simplified Source

```c
static Portal SPI_cursor_open_internal(const char *name, SPIPlanPtr plan,
                                      ParamListInfo paramLI, bool read_only) {
    CachedPlanSource *plansource;
    CachedPlan *cplan;
    Portal portal;
    char *query_string;
    Snapshot snapshot;

    // Validate plan is suitable for cursor operations
    if (!SPI_is_cursor_plan(plan)) {
        // Error handling for invalid plans
        ereport(ERROR, (errcode(ERRCODE_INVALID_CURSOR_DEFINITION),
                       errmsg("cannot open multi-query plan as cursor")));
    }

    plansource = (CachedPlanSource *) linitial(plan->plancache_list);

    // Initialize SPI context
    if (_SPI_begin_call(true) < 0)
        elog(ERROR, "SPI_cursor_open called while not connected");

    // Create portal with name or auto-generate
    if (name == NULL || name[0] == '\0')
        portal = CreateNewPortal();
    else
        portal = CreatePortal(name, false, false);

    // Set up portal with query and plan
    query_string = MemoryContextStrdup(portal->portalContext, plansource->query_string);
    cplan = GetCachedPlan(plansource, paramLI, NULL, _SPI_current->queryEnv);

    PortalDefineQuery(portal, NULL, query_string, plansource->commandTag,
                     cplan->stmt_list, cplan);

    // Configure cursor options (scroll behavior, etc.)
    portal->cursorOptions = plan->cursor_options;
    if (!(portal->cursorOptions & (CURSOR_OPT_SCROLL | CURSOR_OPT_NO_SCROLL))) {
        // Auto-determine scroll capability
        portal->cursorOptions |= CURSOR_OPT_SCROLL; // Simplified logic
    }

    // Validate read-only requirements
    if (read_only) {
        // Check all statements in plan are read-only
        // Simplified validation logic
    }

    // Set up execution snapshot
    snapshot = read_only ? GetActiveSnapshot() : GetTransactionSnapshot();

    // Copy parameters if provided
    if (paramLI) {
        paramLI = copyParamList(paramLI);
    }

    // Start portal execution
    PortalStart(portal, paramLI, 0, snapshot);

    // Clean up and return
    _SPI_end_call(true);
    return portal;
}
```