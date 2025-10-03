# ExecVacuum

## Location
[src/backend/commands/vacuum.c:148-478](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/vacuum.c#L148-L478)

## Overview
Primary entry point for manual VACUUM and ANALYZE commands, serving as a preparation wrapper that parses options and delegates to the vacuum() function.

## Definition

```c
void
ExecVacuum(ParseState *pstate, VacuumStmt *vacstmt, bool isTopLevel)
```
## Detailed Description
ExecVacuum is the main preparation and coordination function for user-initiated VACUUM and ANALYZE commands. It parses the SQL statement's options, validates parameters, constructs the VacuumParams structure, and creates necessary memory contexts and buffer strategies before calling the core vacuum() function.

The function handles extensive option parsing including verbose mode, skip_locked, buffer usage limits, parallel processing, index cleanup strategies, and various specialized vacuum modes. It performs comprehensive validation of option combinations, ensuring incompatible options are rejected with appropriate error messages.

Key responsibilities include:
- Parsing and validating all VACUUM/ANALYZE options from the SQL statement
- Setting up VacuumParams structure with appropriate flags and values
- Creating a cross-transaction memory context for vacuum operations
- Establishing buffer access strategies for efficient I/O management
- Enforcing business rules and option compatibility constraints
- Delegating actual vacuum work to the vacuum() function

## Parameters / Member Variables
- `*pstate`: ParseState containing parser context information for error reporting
- `*vacstmt`: VacuumStmt structure containing the parsed VACUUM/ANALYZE statement with options and target relations
- `isTopLevel`: Boolean indicating whether this is a top-level command (affects transaction handling)
## Dependencies
- Functions called/Symbols referenced:
  - [vacuum](../v/vacuum.md) (core vacuum implementation)
  - [defGetBoolean](../d/defGetBoolean.md), defGetString, defGetInt32 (option parsing utilities)
  - [parse_int](../p/parse_int.md) (string to integer conversion with units)
  - [get_vacoptval_from_boolean](../g/get_vacoptval_from_boolean.md) (option value conversion)
  - AllocSetContextCreate (memory context creation)
  - [GetAccessStrategyWithSize](../G/GetAccessStrategyWithSize.md) (buffer strategy creation)
  - [MemoryContextDelete](../M/MemoryContextDelete.md) (cleanup)
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md) (main utility command dispatcher)

## Notes and Other Information
- Supports extensive option validation including buffer usage limits, parallel worker counts, and option compatibility
- Creates a special "Vacuum" memory context as a child of PortalContext for cross-transaction storage
- Handles both VACUUM and ANALYZE operations through unified option processing
- Enforces numerous business rules: VACUUM FULL cannot be parallelized, BUFFER_USAGE_LIMIT incompatible with VACUUM FULL (except when combined with ANALYZE)
- Buffer usage limits are validated against MIN_BAS_VAC_RING_SIZE_KB and MAX_BAS_VAC_RING_SIZE_KB constants
- Supports specialized database-only statistics operations through ONLY_DATABASE_STATS option
- Default parallel vacuum is enabled (nworkers = 0 means auto-detect optimal worker count)

## Simplified Source

```c
void ExecVacuum(ParseState *pstate, VacuumStmt *vacstmt, bool isTopLevel)
{
    VacuumParams params;
    BufferAccessStrategy bstrategy = NULL;
    bool verbose = false, skip_locked = false, analyze = false;
    bool freeze = false, full = false, disable_page_skipping = false;
    bool process_main = true, process_toast = true;
    int ring_size = -1;
    bool skip_database_stats = false, only_database_stats = false;
    MemoryContext vac_context;
    ListCell *lc;

    // Initialize default parameters
    params.index_cleanup = VACOPTVALUE_UNSPECIFIED;
    params.truncate = VACOPTVALUE_UNSPECIFIED;
    params.nworkers = 0;  // Default parallel vacuum enabled
    params.toast_parent = InvalidOid;

    // Parse all vacuum/analyze options from SQL statement
    foreach(lc, vacstmt->options)
    {
        DefElem *opt = (DefElem *) lfirst(lc);

        if (strcmp(opt->defname, "verbose") == 0)
            verbose = defGetBoolean(opt);
        else if (strcmp(opt->defname, "skip_locked") == 0)
            skip_locked = defGetBoolean(opt);
        else if (strcmp(opt->defname, "buffer_usage_limit") == 0)
        {
            // Parse and validate buffer usage limit
            char *vac_buffer_size = defGetString(opt);
            int result;
            if (!parse_int(vac_buffer_size, &result, GUC_UNIT_KB, NULL) ||
                (result != 0 && (result < MIN_BAS_VAC_RING_SIZE_KB ||
                                result > MAX_BAS_VAC_RING_SIZE_KB)))
                ereport(ERROR, /* invalid buffer usage limit */);
            ring_size = result;
        }
        // Parse vacuum-specific options
        else if (vacstmt->is_vacuumcmd)
        {
            if (strcmp(opt->defname, "analyze") == 0)
                analyze = defGetBoolean(opt);
            else if (strcmp(opt->defname, "freeze") == 0)
                freeze = defGetBoolean(opt);
            else if (strcmp(opt->defname, "full") == 0)
                full = defGetBoolean(opt);
            else if (strcmp(opt->defname, "parallel") == 0)
            {
                int nworkers = defGetInt32(opt);
                if (nworkers < 0 || nworkers > MAX_PARALLEL_WORKER_LIMIT)
                    ereport(ERROR, /* invalid parallel worker count */);
                params.nworkers = (nworkers == 0) ? -1 : nworkers;
            }
            // Handle other vacuum options...
        }
        else
            ereport(ERROR, /* unrecognized option */);
    }

    // Build options bitmask from parsed boolean flags
    params.options =
        (vacstmt->is_vacuumcmd ? VACOPT_VACUUM : VACOPT_ANALYZE) |
        (verbose ? VACOPT_VERBOSE : 0) |
        (skip_locked ? VACOPT_SKIP_LOCKED : 0) |
        (analyze ? VACOPT_ANALYZE : 0) |
        (freeze ? VACOPT_FREEZE : 0) |
        (full ? VACOPT_FULL : 0) |
        /* ... other option flags ... */;

    // Validate option combinations
    if ((params.options & VACOPT_FULL) && params.nworkers > 0)
        ereport(ERROR, /* VACUUM FULL cannot be parallel */);

    if (ring_size != -1 && (params.options & VACOPT_FULL) &&
        !(params.options & VACOPT_ANALYZE))
        ereport(ERROR, /* BUFFER_USAGE_LIMIT incompatible with VACUUM FULL */);

    // Set freeze ages based on FREEZE option
    if (params.options & VACOPT_FREEZE)
    {
        params.freeze_min_age = 0;
        params.freeze_table_age = 0;
        params.multixact_freeze_min_age = 0;
        params.multixact_freeze_table_age = 0;
    }
    else
    {
        // Use default values
        params.freeze_min_age = -1;
        params.freeze_table_age = -1;
        params.multixact_freeze_min_age = -1;
        params.multixact_freeze_table_age = -1;
    }

    // Set other parameter defaults
    params.is_wraparound = false;
    params.log_min_duration = -1;

    // Create cross-transaction memory context
    vac_context = AllocSetContextCreate(PortalContext, "Vacuum",
                                       ALLOCSET_DEFAULT_SIZES);

    // Create buffer strategy if needed (not for VACUUM FULL or database stats only)
    if ((params.options & (VACOPT_ONLY_DATABASE_STATS | VACOPT_FULL)) == 0 ||
        (params.options & VACOPT_ANALYZE) != 0)
    {
        MemoryContext old_context = MemoryContextSwitchTo(vac_context);

        if (ring_size == -1)
            ring_size = VacuumBufferUsageLimit;
        bstrategy = GetAccessStrategyWithSize(BAS_VACUUM, ring_size);

        MemoryContextSwitchTo(old_context);
    }

    // Delegate to core vacuum implementation
    vacuum(vacstmt->rels, &params, bstrategy, vac_context, isTopLevel);

    // Cleanup
    MemoryContextDelete(vac_context);
}
```