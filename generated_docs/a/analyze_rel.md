# analyze_rel

## Location
[src/backend/commands/analyze.c:111-279](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/analyze.c#L111-L279)

## Overview
The main entry point function for analyzing a single relation (table, materialized view, or foreign table) to gather statistics for the query planner.

## Definition

```c
void
analyze_rel(Oid relid, RangeVar *relation,
			VacuumParams *params, List *va_cols, bool in_outer_xact,
			BufferAccessStrategy bstrategy)
```
## Detailed Description
This function orchestrates the analysis of a single relation by performing several validation checks and then delegating to  for the actual statistics gathering. It handles different relation types (regular tables, materialized views, foreign tables, and partitioned tables) and implements proper locking mechanisms to prevent concurrent ANALYZE operations. For partitioned tables, it performs both non-recursive analysis (skipped for partitioned tables as they contain no data) and recursive analysis of child partitions when applicable.

The function performs comprehensive validation including privilege checks, relation type verification, and handles special cases like temporary tables of other backends and the system statistics table .

## Parameters / Member Variables
- `relid`: OID of the relation to analyze
- `*relation`: RangeVar containing relation name information for error reporting (may be stale)
- `*params`: Vacuum parameters structure containing analysis options and configuration
- `*va_cols`: List of specific columns to analyze (NULL for all columns)
- `in_outer_xact`: Boolean indicating if running within an outer transaction
- `bstrategy`: Buffer access strategy for controlling buffer replacement during analysis
## Dependencies
- Functions called/Symbols referenced:
  - : Opens and locks the relation for analysis
  - : Checks user privileges for analysis
  - : Closes the relation and manages locks
  - : Performs the actual statistics collection
  - /: Progress reporting
  - : Standard row sampling function for regular tables
  - : Foreign table analysis support
- Called from (representative examples):
  - : Main vacuum command entry point

## Notes and Other Information
- Uses ShareUpdateExclusiveLock to prevent concurrent ANALYZE operations on the same relation
- Skips analysis for temporary tables of other backends and the pg_statistic system table
- Supports foreign table analysis through FDW-specific hooks
- For partitioned tables, performs recursive analysis of child partitions when  is true
- Maintains locks until transaction commit to ensure consistency of statistics updates

## Simplified Source

```c
void analyze_rel(Oid relid, RangeVar *relation,
                VacuumParams *params, List *va_cols, bool in_outer_xact,
                BufferAccessStrategy bstrategy) {
    Relation onerel;
    int elevel;
    AcquireSampleRowsFunc acquirefunc = NULL;
    BlockNumber relpages = 0;

    // Set logging level based on verbose option
    elevel = (params->options & VACOPT_VERBOSE) ? INFO : DEBUG2;
    vac_strategy = bstrategy;

    CHECK_FOR_INTERRUPTS();

    // Open relation with ShareUpdateExclusiveLock to prevent concurrent ANALYZE
    onerel = vacuum_open_relation(relid, relation, params->options & ~(VACOPT_VACUUM),
                                 params->log_min_duration >= 0,
                                 ShareUpdateExclusiveLock);
    if (!onerel)
        return;

    // Check privileges for analysis operation
    if (!vacuum_is_permitted_for_relation(RelationGetRelid(onerel),
                                         onerel->rd_rel,
                                         params->options & ~VACOPT_VACUUM)) {
        relation_close(onerel, ShareUpdateExclusiveLock);
        return;
    }

    // Skip temp tables of other backends and pg_statistic table
    if (RELATION_IS_OTHER_TEMP(onerel) ||
        RelationGetRelid(onerel) == StatisticRelationId) {
        relation_close(onerel, ShareUpdateExclusiveLock);
        return;
    }

    // Set up analysis function based on relation type
    if (onerel->rd_rel->relkind == RELKIND_RELATION ||
        onerel->rd_rel->relkind == RELKIND_MATVIEW) {
        // Regular table or materialized view
        acquirefunc = acquire_sample_rows;
        relpages = RelationGetNumberOfBlocks(onerel);
    } else if (onerel->rd_rel->relkind == RELKIND_FOREIGN_TABLE) {
        // Foreign table: check if FDW supports analysis
        FdwRoutine *fdwroutine = GetFdwRoutineForRelation(onerel, false);
        bool ok = false;

        if (fdwroutine->AnalyzeForeignTable != NULL)
            ok = fdwroutine->AnalyzeForeignTable(onerel, &acquirefunc, &relpages);

        if (!ok) {
            ereport(WARNING, (errmsg("skipping \"%s\" --- cannot analyze this foreign table",
                                   RelationGetRelationName(onerel))));
            relation_close(onerel, ShareUpdateExclusiveLock);
            return;
        }
    } else if (onerel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE) {
        // Partitioned table: will do recursive analysis below
    } else {
        // Unsupported relation type
        if (!(params->options & VACOPT_VACUUM))
            ereport(WARNING, (errmsg("skipping \"%s\" --- cannot analyze non-tables",
                                   RelationGetRelationName(onerel))));
        relation_close(onerel, ShareUpdateExclusiveLock);
        return;
    }

    // Initialize progress reporting
    pgstat_progress_start_command(PROGRESS_COMMAND_ANALYZE,
                                 RelationGetRelid(onerel));

    // Perform non-recursive analysis (skip for partitioned tables)
    if (onerel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
        do_analyze_rel(onerel, params, va_cols, acquirefunc,
                      relpages, false, in_outer_xact, elevel);

    // Perform recursive analysis if relation has child tables
    if (onerel->rd_rel->relhassubclass)
        do_analyze_rel(onerel, params, va_cols, acquirefunc, relpages,
                      true, in_outer_xact, elevel);

    // Close relation but keep lock until commit
    relation_close(onerel, NoLock);
    pgstat_progress_end_command();
}
```