# PLy_procedure_get

## Location
[src/pl/plpython/plpy_procedure.c:69-132](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_procedure.c#L69-L132)

## Overview
Retrieves a cached PLyProcedure or creates, stores, and returns a new one, serving as the main entry point for obtaining compiled PL/Python procedures with intelligent caching and validation.

## Definition
```c
PLyProcedure *PLy_procedure_get(Oid fn_oid, Oid fn_rel, bool is_trigger)
```

## Detailed Description
This function implements a sophisticated caching mechanism for PL/Python procedures. It first attempts to retrieve a procedure from the cache using a composite key of function OID and relation OID. If not found, it creates a new procedure using PLy_procedure_create. The function includes validation logic to ensure cached procedures are still valid against the current system catalog, and handles exception safety to prevent leaving invalid entries in the cache. Special handling is implemented for trigger functions during validation when the target relation is unknown.

## Parameters / Member Variables
- `fn_oid`: OID of the function to retrieve/create
- `fn_rel`: OID of the relation this function triggers on, or InvalidOid for non-trigger functions or during validation
- `is_trigger`: Boolean flag indicating whether this is a trigger function

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system catalog lookup)
  - [hash_search](../h/hash_search.md) (hash table operations with HASH_ENTER and HASH_REMOVE)
  - [PLy_procedure_create](PLy_procedure_create.md) (creates new procedure instances)
  - [PLy_procedure_valid](PLy_procedure_valid.md) (validates cached procedures)
  - [PLy_procedure_delete](PLy_procedure_delete.md) (cleans up invalid procedures)
  - PG_TRY/PG_CATCH/PG_END_TRY (PostgreSQL exception handling)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (cleanup system cache)
- Called from (representative examples):
  - [plpython3_validator](../p/plpython3_validator.md) (function validation)
  - [plpython3_call_handler](../p/plpython3_call_handler.md) (function execution)

## Notes and Other Information
- Uses composite caching strategy based on both function OID and relation OID
- Disables caching for trigger functions during validation when relation is unknown
- Implements exception-safe caching that prevents orphaned cache entries
- Validates cached procedures against current system catalog to handle schema changes
- Critical for PL/Python performance by avoiding repeated compilation of the same functions
- The returned procedure object is managed by the cache and should not be freed directly by callers

## Simplified Source

```c
PLyProcedure *PLy_procedure_get(Oid fn_oid, Oid fn_rel, bool is_trigger) {
    bool use_cache = !(is_trigger && fn_rel == InvalidOid);
    HeapTuple procTup;
    PLyProcedureKey key;
    PLyProcedureEntry *entry = NULL;
    PLyProcedure *proc = NULL;
    bool found = false;

    // Look up function in system catalog
    procTup = SearchSysCache1(PROCOID, ObjectIdGetDatum(fn_oid));
    if (!HeapTupleIsValid(procTup))
        elog(ERROR, "cache lookup failed for function %u", fn_oid);

    // Check cache if we have enough info (not during trigger validation)
    if (use_cache) {
        key.fn_oid = fn_oid;
        key.fn_rel = fn_rel;
        entry = hash_search(PLy_procedure_cache, &key, HASH_ENTER, &found);
        proc = entry->proc;
    }

    PG_TRY(); {
        if (!found) {
            // Create new procedure and cache it
            proc = PLy_procedure_create(procTup, fn_oid, is_trigger);
            if (use_cache)
                entry->proc = proc;
        }
        else if (!PLy_procedure_valid(proc, procTup)) {
            // Cached version is stale, recreate
            entry->proc = NULL;
            if (proc)
                PLy_procedure_delete(proc);
            proc = PLy_procedure_create(procTup, fn_oid, is_trigger);
            entry->proc = proc;
        }
        // Else: found valid cached procedure, use it
    }
    PG_CATCH(); {
        // Clean up incomplete cache entry on error
        if (use_cache)
            hash_search(PLy_procedure_cache, &key, HASH_REMOVE, NULL);
        PG_RE_THROW();
    }
    PG_END_TRY();

    ReleaseSysCache(procTup);
    return proc;
}
```