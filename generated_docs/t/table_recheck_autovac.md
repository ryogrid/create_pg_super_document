# table_recheck_autovac

## Location
src/backend/postmaster/autovacuum.c: 2732 - 2876

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