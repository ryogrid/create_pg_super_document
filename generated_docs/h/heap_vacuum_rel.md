# heap_vacuum_rel

## Location
[src/backend/access/heap/vacuumlazy.c:295-815](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/vacuumlazy.c#L295-L815)

## Overview
heap_vacuum_rel performs VACUUM operation for one heap relation, setting up the environment and orchestrating the entire vacuum process including heap scanning, index maintenance, and statistics updates.

## Definition

```c
void
heap_vacuum_rel(Relation rel, VacuumParams *params,
				BufferAccessStrategy bstrategy)
```
## Detailed Description
heap_vacuum_rel is the main entry point for vacuuming a single heap relation. It performs comprehensive setup and coordination of the vacuum process:

1. **Initialization Phase**: Sets up the LVRelState structure containing all vacuum-related state, initializes error callbacks, opens indexes, and configures vacuum options based on parameters.

2. **Cutoff Determination**: Calculates transaction ID and multixact ID cutoffs that determine which tuples are considered dead and which XIDs/MXIDs should be frozen.

3. **Core Vacuum Work**: Calls lazy_scan_heap to perform the actual heap scanning, pruning, and vacuuming operations.

4. **Post-Processing**: Updates pg_class entries for the relation and its indexes, optionally truncates the relation, and generates comprehensive statistics reports.

The function handles both aggressive and non-aggressive vacuum modes, supports parallel vacuum operations, implements failsafe mechanisms, and provides detailed instrumentation and logging.

## Parameters / Member Variables
- `rel`: The heap relation to be vacuumed
- `*params`: VacuumParams structure containing vacuum options and settings
- `bstrategy`: Buffer access strategy to use during vacuum operations
## Dependencies
- Functions called/Symbols referenced:
  - [lazy_scan_heap](../l/lazy_scan_heap.md) (core vacuum work)
  - [vacuum_get_cutoffs](../v/vacuum_get_cutoffs.md) (cutoff calculations)
  - [dead_items_alloc](../d/dead_items_alloc.md) (memory management)
  - [vac_open_indexes](../v/vac_open_indexes.md) / vac_close_indexes (index management)
  - [lazy_check_wraparound_failsafe](../l/lazy_check_wraparound_failsafe.md) (safety checks)
  - [should_attempt_truncation](../s/should_attempt_truncation.md) / lazy_truncate_heap (relation truncation)
  - [update_relstats_all_indexes](../u/update_relstats_all_indexes.md) (statistics updates)
  - [vac_update_relstats](../v/vac_update_relstats.md) (relation statistics)
  - [pgstat_report_vacuum](../p/pgstat_report_vacuum.md) (statistics reporting)

- Called from (representative examples):
  - [SampleHeapTupleVisible](../S/SampleHeapTupleVisible.md) (src/backend/access/heap/heapam_handler.c:2632)
  - HeapScanIsValid (src/include/access/heapam.h:402)

## Notes and Other Information
- The function implements comprehensive error handling with vacuum_error_callback for detailed error reporting
- Supports both verbose and quiet operation modes with detailed logging and instrumentation
- Handles failsafe mechanisms to prevent transaction ID wraparound
- Manages parallel vacuum worker coordination through dead_items_alloc/cleanup
- Updates multiple system catalogs (pg_class) and statistics subsystems
- Implements sophisticated decision-making for index vacuuming bypass optimization
- Source location: src/backend/access/heap/vacuumlazy.c:295-815

## Simplified Source

```c
void heap_vacuum_rel(Relation rel, VacuumParams *params, BufferAccessStrategy bstrategy) {
    LVRelState *vacrel;
    bool verbose, instrument, skipwithvm;
    BlockNumber orig_rel_pages, new_rel_pages, new_rel_allvisible;

    // Initialize verbose and instrumentation settings
    verbose = (params->options & VACOPT_VERBOSE) != 0;
    instrument = (verbose || (AmAutoVacuumWorkerProcess() && params->log_min_duration >= 0));

    if (instrument) {
        // Setup timing and resource usage tracking
        pg_rusage_init(&ru0);
        starttime = GetCurrentTimestamp();
    }

    // Setup vacuum state and error callback
    vacrel = (LVRelState *) palloc0(sizeof(LVRelState));
    vacrel->dbname = get_database_name(MyDatabaseId);
    vacrel->relname = pstrdup(RelationGetRelationName(rel));
    vacrel->rel = rel;

    // Setup error context callback for better error reporting
    errcallback.callback = vacuum_error_callback;
    errcallback.arg = vacrel;
    error_context_stack = &errcallback;

    // Open indexes and setup vacuum configuration
    vac_open_indexes(vacrel->rel, RowExclusiveLock, &vacrel->nindexes, &vacrel->indrels);
    vacrel->bstrategy = bstrategy;

    // Configure vacuum behavior based on parameters
    VacuumFailsafeActive = false;
    vacrel->consider_bypass_optimization = true;
    vacrel->do_index_vacuuming = true;
    vacrel->do_index_cleanup = true;
    vacrel->do_rel_truncate = (params->truncate != VACOPTVALUE_DISABLED);

    if (params->index_cleanup == VACOPTVALUE_DISABLED) {
        vacrel->do_index_vacuuming = false;
        vacrel->do_index_cleanup = false;
    } else if (params->index_cleanup == VACOPTVALUE_ENABLED) {
        vacrel->consider_bypass_optimization = false;
    }

    // Initialize counters and statistics
    vacrel->scanned_pages = 0;
    vacrel->removed_pages = 0;
    vacrel->new_rel_tuples = 0;
    vacrel->new_live_tuples = 0;
    vacrel->tuples_deleted = 0;
    vacrel->tuples_frozen = 0;

    // Get cutoffs for determining dead tuples and freeze thresholds
    vacrel->aggressive = vacuum_get_cutoffs(rel, params, &vacrel->cutoffs);
    vacrel->rel_pages = orig_rel_pages = RelationGetNumberOfBlocks(rel);
    vacrel->vistest = GlobalVisTestFor(rel);

    // Initialize transaction tracking for relfrozenxid advancement
    vacrel->NewRelfrozenXid = vacrel->cutoffs.OldestXmin;
    vacrel->NewRelminMxid = vacrel->cutoffs.OldestMxact;
    vacrel->skippedallvis = false;

    skipwithvm = true;
    if (params->options & VACOPT_DISABLE_PAGE_SKIPPING) {
        vacrel->aggressive = true;
        skipwithvm = false;
    }
    vacrel->skipwithvm = skipwithvm;

    // Report vacuum start
    if (verbose) {
        if (vacrel->aggressive)
            ereport(INFO, (errmsg("aggressively vacuuming \"%s.%s.%s\"",
                                  vacrel->dbname, vacrel->relnamespace, vacrel->relname)));
        else
            ereport(INFO, (errmsg("vacuuming \"%s.%s.%s\"",
                                  vacrel->dbname, vacrel->relnamespace, vacrel->relname)));
    }

    // Allocate memory for dead items and perform safety checks
    lazy_check_wraparound_failsafe(vacrel);
    dead_items_alloc(vacrel, params->nworkers);

    // *** CORE VACUUM WORK ***
    lazy_scan_heap(vacrel);

    // Cleanup memory and end parallel mode
    dead_items_cleanup(vacrel);

    // Update index statistics
    if (vacrel->do_index_cleanup)
        update_relstats_all_indexes(vacrel);

    // Close indexes
    vac_close_indexes(vacrel->nindexes, vacrel->indrels, NoLock);

    // Optionally truncate relation
    if (should_attempt_truncation(vacrel))
        lazy_truncate_heap(vacrel);

    // Prepare final statistics for pg_class update
    new_rel_pages = vacrel->rel_pages;
    visibilitymap_count(rel, &new_rel_allvisible, NULL);
    if (new_rel_allvisible > new_rel_pages)
        new_rel_allvisible = new_rel_pages;

    // Update pg_class entry with new statistics
    vac_update_relstats(rel, new_rel_pages, vacrel->new_live_tuples,
                        new_rel_allvisible, vacrel->nindexes > 0,
                        vacrel->NewRelfrozenXid, vacrel->NewRelminMxid,
                        &frozenxid_updated, &minmulti_updated, false);

    // Report results to statistics system
    pgstat_report_vacuum(RelationGetRelid(rel), rel->rd_rel->relisshared,
                         Max(vacrel->new_live_tuples, 0),
                         vacrel->recently_dead_tuples + vacrel->missed_dead_tuples);

    // Generate detailed vacuum report if instrumentation enabled
    if (instrument) {
        TimestampTz endtime = GetCurrentTimestamp();

        if (verbose || params->log_min_duration == 0 ||
            TimestampDifferenceExceeds(starttime, endtime, params->log_min_duration)) {
            // Log comprehensive vacuum statistics including:
            // - Pages removed/remaining/scanned
            // - Tuples removed/remaining/dead
            // - Index scan information
            // - Performance metrics (timing, I/O rates, buffer usage)
            // - WAL usage statistics
        }
    }

    // Cleanup error context
    error_context_stack = errcallback.previous;
}
```