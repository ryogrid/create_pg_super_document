# table_recheck_autovac

## Location
[src/backend/postmaster/autovacuum.c:2732-2876](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/autovacuum.c#L2732-L2876)

## Overview
table_recheck_autovac rechecks whether a table still needs vacuum or analyze operations and returns a configured autovac_table structure if maintenance is required.

## Definition
```c
static autovac_table *table_recheck_autovac(Oid relid, HTAB *table_toast_map,
                                           TupleDesc pg_class_desc,
                                           int effective_multixact_freeze_max_age)
```

## Detailed Description
This function serves as a critical validation step in the autovacuum process, confirming that a previously identified table still requires maintenance work before actual vacuum/analyze operations begin. It handles the race condition where other processes might have performed maintenance on the table between initial identification and processing.

The function retrieves fresh statistics and configuration for the specified relation, extracts autovacuum options (including handling TOAST table inheritance from main tables), and creates a fully configured autovac_table structure with appropriate vacuum parameters. It respects both table-specific reloptions and system-wide defaults, calculating freeze ages and vacuum costs appropriately.

The returned autovac_table contains all necessary parameters for the vacuum/analyze operation, including options flags, freeze ages, cost parameters, and balancing settings. The function ensures that wraparound prevention vacuums cannot be skipped due to locks.

## Parameters / Member Variables
- `relid`: OID of the relation to recheck
- `table_toast_map`: Hash table mapping TOAST tables to their main table options
- `pg_class_desc`: Tuple descriptor for pg_class relation
- `effective_multixact_freeze_max_age`: Effective freeze age threshold for multixacts
- Returns: Pointer to autovac_table structure if work needed, NULL otherwise

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCacheCopy1 (fetch relation's pg_class tuple)
  - [extract_autovac_opts](../e/extract_autovac_opts.md) (extract autovacuum options from reloptions)
  - [recheck_relation_needs_vacanalyze](../r/recheck_relation_needs_vacanalyze.md) (determine if maintenance is needed)
  - [hash_search](../h/hash_search.md) (lookup TOAST table options)
  - [palloc](../p/palloc.md) (memory allocation for autovac_table)
  - [heap_freetuple](../h/heap_freetuple.md) (free catalog tuple)
- Called from (representative examples):
  - [do_autovacuum](../d/do_autovacuum.md) (during table processing to recheck maintenance needs)

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCacheCopy1/extract_autovac_opts/recheck_relation_needs_vacanalyze
  - [hash_search](../h/hash_search.md) (for TOAST table option lookup)  
  - [palloc](../p/palloc.md)/heap_freetuple (memory management)
  - VACOPT_* constants (vacuum option flags)
- Called from (representative examples):
  - [do_autovacuum](../d/do_autovacuum.md) (recheck table maintenance needs before processing)

## Notes and Other Information
- Returns NULL if the relation no longer exists or no longer needs maintenance
- Handles TOAST tables by inheriting options from main tables when no specific options are set
- Sets appropriate vacuum options including VACOPT_SKIP_DATABASE_STATS to avoid duplicate statistics updates
- Disables parallel vacuum for autovacuum operations (nworkers = -1)
- Implements cost balancing logic - disabled when table has specific cost parameters
- Sets index_cleanup and truncate as unspecified initially, to be filled later from reloptions
- For wraparound prevention, sets VACOPT_SKIP_LOCKED to false to ensure vacuum proceeds
- The returned structure does not have name fields set (at_relname, at_nspname, at_datname are NULL)
- Properly handles memory management for extracted autovacuum options

## Simplified Source

```c
static autovac_table *
table_recheck_autovac(Oid relid, HTAB *table_toast_map,
                      TupleDesc pg_class_desc,
                      int effective_multixact_freeze_max_age)
{
    Form_pg_class classForm;
    HeapTuple classTup;
    bool dovacuum, doanalyze, wraparound;
    autovac_table *tab = NULL;
    AutoVacOpts *avopts;
    bool free_avopts = false;

    // Fetch fresh relation metadata
    classTup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(classTup))
        return NULL;
    classForm = (Form_pg_class) GETSTRUCT(classTup);

    // Get autovac options, checking TOAST table inheritance
    avopts = extract_autovac_opts(classTup, pg_class_desc);
    if (avopts)
        free_avopts = true;
    else if (classForm->relkind == RELKIND_TOASTVALUE && table_toast_map != NULL)
    {
        // Inherit options from main table for TOAST tables
        av_relation *hentry;
        bool found;
        hentry = hash_search(table_toast_map, &relid, HASH_FIND, &found);
        if (found && hentry->ar_hasrelopts)
            avopts = &hentry->ar_reloptions;
    }

    // Recheck if maintenance is needed with fresh stats
    recheck_relation_needs_vacanalyze(relid, avopts, classForm,
                                      effective_multixact_freeze_max_age,
                                      &dovacuum, &doanalyze, &wraparound);

    // Create autovac_table if work is needed
    if (doanalyze || dovacuum)
    {
        tab = palloc(sizeof(autovac_table));
        tab->at_relid = relid;
        tab->at_sharedrel = classForm->relisshared;

        // Configure vacuum parameters
        tab->at_params.options =
            (dovacuum ? (VACOPT_VACUUM | VACOPT_PROCESS_MAIN | VACOPT_SKIP_DATABASE_STATS) : 0) |
            (doanalyze ? VACOPT_ANALYZE : 0) |
            (!wraparound ? VACOPT_SKIP_LOCKED : 0);

        // Set freeze ages from options or defaults
        tab->at_params.freeze_min_age = (avopts && avopts->freeze_min_age >= 0) ?
            avopts->freeze_min_age : default_freeze_min_age;
        tab->at_params.freeze_table_age = (avopts && avopts->freeze_table_age >= 0) ?
            avopts->freeze_table_age : default_freeze_table_age;

        // Configure cost parameters and balancing
        tab->at_storage_param_vac_cost_limit = avopts ? avopts->vacuum_cost_limit : 0;
        tab->at_storage_param_vac_cost_delay = avopts ? avopts->vacuum_cost_delay : -1;
        tab->at_dobalance = !(avopts && (avopts->vacuum_cost_limit > 0 ||
                                         avopts->vacuum_cost_delay >= 0));

        // Initialize other fields
        tab->at_params.nworkers = -1; // No parallel vacuum for autovacuum
        tab->at_params.is_wraparound = wraparound;
        tab->at_relname = tab->at_nspname = tab->at_datname = NULL;
    }

    // Clean up
    if (free_avopts) pfree(avopts);
    heap_freetuple(classTup);
    return tab;
}
```