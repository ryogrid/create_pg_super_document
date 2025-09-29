# fmgr_security_definer

## Location
[src/backend/utils/fmgr/fmgr.c:632-791](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L632-L791)

## Overview
A function handler that implements security-definer execution, configuration parameter management, and plugin hooks for PostgreSQL functions requiring elevated privileges or special execution contexts.

## Definition

```c
struct fmgr_security_definer_cache *volatile fcache;
```
## Detailed Description
The  function serves as a sophisticated wrapper for executing PostgreSQL functions that require special security or configuration contexts. It handles three main responsibilities: security-definer execution (running functions with the privileges of their owner rather than the caller), applying function-specific configuration parameters (proconfig), and invoking function manager hooks for plugins.

The function operates by creating a cache of execution context information on first invocation, then temporarily switching user context and configuration settings before calling the actual target function. It uses PostgreSQL's exception handling system (PG_TRY/PG_CATCH) to ensure proper cleanup of security contexts even when the wrapped function fails.

The security-definer mechanism allows functions to execute with the privileges of their creator (owner) rather than the current user, enabling controlled privilege escalation. Configuration parameters allow functions to temporarily override GUC (Grand Unified Configuration) settings during execution. Plugin hooks provide extensibility points for third-party code to monitor or modify function execution.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that expands to 

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md)
  - [fmgr_info_cxt_security](fmgr_info_cxt_security.md)  
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - [TransformGUCArray](../T/TransformGUCArray.md)
  - [get_config_handle](../g/get_config_handle.md)
  - [GetUserIdAndSecContext](../G/GetUserIdAndSecContext.md)
  - [SetUserIdAndSecContext](../S/SetUserIdAndSecContext.md)
  - [NewGUCNestLevel](../N/NewGUCNestLevel.md)
  - [set_config_with_handle](../s/set_config_with_handle.md)
  - FunctionCallInvoke
  - [pgstat_init_function_usage](../p/pgstat_init_function_usage.md)
  - [pgstat_end_function_usage](../p/pgstat_end_function_usage.md)
  - [AtEOXact_GUC](../A/AtEOXact_GUC.md)
- Called from (representative examples):
  - [fmgr_info_cxt_security](fmgr_info_cxt_security.md)

## Notes and Other Information
- Caches execution context information in fn_extra for performance
- Handles both security-definer and configuration parameter functionality
- Uses PostgreSQL's GUC nesting mechanism for configuration changes
- Implements proper exception handling to restore security contexts on errors
- Supports function manager hooks for plugin extensibility
- Not re-entrant due to flinfo manipulation, but fcinfo itself isn't re-entrant either
- Critical component for PostgreSQL's function security and configuration system
- Manages statistics collection for wrapped function execution

## Simplified Source

```c
extern Datum fmgr_security_definer(PG_FUNCTION_ARGS)
{
    Datum result;
    struct fmgr_security_definer_cache *volatile fcache;
    FmgrInfo *save_flinfo;
    Oid save_userid;
    int save_sec_context;
    volatile int save_nestlevel;
    PgStat_FunctionCallUsage fcusage;

    // Initialize cache on first call
    if (!fcinfo->flinfo->fn_extra) {
        HeapTuple tuple;
        Form_pg_proc procedureStruct;

        // Allocate and initialize cache
        fcache = MemoryContextAllocZero(fcinfo->flinfo->fn_mcxt, sizeof(*fcache));

        // Set up actual function info
        fmgr_info_cxt_security(fcinfo->flinfo->fn_oid, &fcache->flinfo,
                               fcinfo->flinfo->fn_mcxt, true);

        // Get function metadata from pg_proc
        tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(fcinfo->flinfo->fn_oid));
        if (!HeapTupleIsValid(tuple))
            elog(ERROR, "cache lookup failed for function %u", fcinfo->flinfo->fn_oid);

        procedureStruct = (Form_pg_proc) GETSTRUCT(tuple);

        // Set user ID for security definer functions
        if (procedureStruct->prosecdef)
            fcache->userid = procedureStruct->proowner;

        // Extract and transform configuration parameters
        Datum datum = SysCacheGetAttr(PROCOID, tuple, Anum_pg_proc_proconfig, &isnull);
        if (!isnull) {
            // Process configuration array and create handles
            // ... (configuration processing code) ...
        }

        ReleaseSysCache(tuple);
        fcinfo->flinfo->fn_extra = fcache;
    } else {
        fcache = fcinfo->flinfo->fn_extra;
    }

    // Save current security context
    GetUserIdAndSecContext(&save_userid, &save_sec_context);
    if (fcache->configNames != NIL)
        save_nestlevel = NewGUCNestLevel();

    // Switch to function owner's privileges if needed
    if (OidIsValid(fcache->userid))
        SetUserIdAndSecContext(fcache->userid,
                               save_sec_context | SECURITY_LOCAL_USERID_CHANGE);

    // Apply configuration parameters
    // ... (GUC setting code) ...

    // Execute function with proper error handling
    save_flinfo = fcinfo->flinfo;
    PG_TRY();
    {
        fcinfo->flinfo = &fcache->flinfo;
        pgstat_init_function_usage(fcinfo, &fcusage);
        result = FunctionCallInvoke(fcinfo);
        pgstat_end_function_usage(&fcusage, /* completion check */);
    }
    PG_CATCH();
    {
        fcinfo->flinfo = save_flinfo;
        // Cleanup and re-throw
        PG_RE_THROW();
    }
    PG_END_TRY();

    // Restore original context
    fcinfo->flinfo = save_flinfo;
    if (fcache->configNames != NIL)
        AtEOXact_GUC(true, save_nestlevel);
    if (OidIsValid(fcache->userid))
        SetUserIdAndSecContext(save_userid, save_sec_context);

    return result;
}
```