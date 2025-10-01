# BuildRelationExtStatistics

## Location
[src/backend/statistics/extended_stats.c:112-264](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/extended_stats.c#L112-L264)

## Overview
BuildRelationExtStatistics computes and stores extended statistics objects for a relation based on sampled data, handling various types of multi-column statistics including n-distinct, dependencies, MCV lists, and expression statistics.

## Definition

```c
void
BuildRelationExtStatistics(Relation onerel, bool inh, double totalrows,
						   int numrows, HeapTuple *rows,
						   int natts, VacAttrStats **vacattrstats)
```
## Detailed Description
This function serves as the main entry point for building extended statistics during the ANALYZE command. It fetches extended statistics definitions from pg_statistic_ext catalog, validates that the required columns have been analyzed, and computes the requested statistics types. For each statistics object, it determines an appropriate statistics target, builds the requested statistics (n-distinct, dependencies, MCV lists, or expression statistics), and stores the results back into the system catalogs. The function also provides progress reporting during extended statistics computation.

The function operates in a temporary memory context to manage memory efficiently during statistics computation, resetting the context after each statistics object is processed.

## Parameters / Member Variables
- : The relation for which to build extended statistics
- : Whether to include inheritance hierarchy statistics  
- : Total number of rows in the relation
- : Number of sampled rows available for computation
- : Array of sampled HeapTuple data
- : Number of attributes being analyzed
- : Array of per-column statistics information

## Dependencies
- Functions called/Symbols referenced:
  - [fetch_statentries_for_relation](../f/fetch_statentries_for_relation.md)
  - [lookup_var_attr_stats](../l/lookup_var_attr_stats.md)
  - [statext_compute_stattarget](../s/statext_compute_stattarget.md)
  - [make_build_data](../m/make_build_data.md)
  - [statext_ndistinct_build](../s/statext_ndistinct_build.md)
  - [statext_dependencies_build](../s/statext_dependencies_build.md)
  - [statext_mcv_build](../s/statext_mcv_build.md)
  - [compute_expr_stats](../c/compute_expr_stats.md)
  - [statext_store](../s/statext_store.md)
  - AllocSetContextCreate
  - [pgstat_progress_update_multi_param](../p/pgstat_progress_update_multi_param.md)
- Called from:
  - [do_analyze_rel](../d/do_analyze_rel.md) (in src/backend/commands/analyze.c:605)

## Notes and Other Information
- Returns early if no columns are being analyzed (natts == 0)
- Skips statistics objects that cannot be computed due to missing column analysis
- Issues warnings for incomputable statistics objects unless running in autovacuum
- Respects statistics target of 0 by preserving existing statistics values
- Uses a dedicated memory context for efficient memory management
- Provides progress reporting through the statistics analysis progress infrastructure
- Handles four types of extended statistics: NDISTINCT, DEPENDENCIES, MCV, and EXPRESSIONS

## Simplified Source

```c
void BuildRelationExtStatistics(Relation onerel, bool inh, double totalrows,
                               int numrows, HeapTuple *rows,
                               int natts, VacAttrStats **vacattrstats) {
    Relation pg_stext;
    List *statslist;
    MemoryContext cxt, oldcxt;
    int64 ext_cnt = 0;

    // Early return if no columns to analyze
    if (!natts)
        return;

    // Get list of extended statistics objects for this relation
    pg_stext = table_open(StatisticExtRelationId, RowExclusiveLock);
    statslist = fetch_statentries_for_relation(pg_stext, RelationGetRelid(onerel));

    // Create memory context for statistics building
    cxt = AllocSetContextCreate(CurrentMemoryContext, "BuildRelationExtStatistics", ALLOCSET_DEFAULT_SIZES);
    oldcxt = MemoryContextSwitchTo(cxt);

    // Report progress if we have statistics to compute
    if (statslist != NIL) {
        const int index[] = {PROGRESS_ANALYZE_PHASE, PROGRESS_ANALYZE_EXT_STATS_TOTAL};
        const int64 val[] = {PROGRESS_ANALYZE_PHASE_COMPUTE_EXT_STATS, list_length(statslist)};
        pgstat_progress_update_multi_param(2, index, val);
    }

    // Process each extended statistics object
    foreach(lc, statslist) {
        StatExtEntry *stat = (StatExtEntry *) lfirst(lc);
        VacAttrStats **stats;
        int stattarget;

        // Check if we can build stats for the required columns
        stats = lookup_var_attr_stats(onerel, stat->columns, stat->exprs, natts, vacattrstats);
        if (!stats) {
            // Issue warning unless in autovacuum
            if (!AmAutoVacuumWorkerProcess()) {
                ereport(WARNING, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                    errmsg("statistics object \"%s.%s\" could not be computed for relation \"%s.%s\"",
                           stat->schema, stat->name,
                           get_namespace_name(onerel->rd_rel->relnamespace),
                           RelationGetRelationName(onerel)),
                    errtable(onerel)));
            }
            continue;
        }

        // Compute statistics target for this object
        stattarget = statext_compute_stattarget(stat->stattarget, bms_num_members(stat->columns), stats);

        // Skip if statistics target is 0
        if (stattarget == 0)
            continue;

        // Prepare data for statistics computation
        StatsBuildData *data = make_build_data(onerel, stat, numrows, rows, stats, stattarget);

        // Initialize statistics objects
        MVNDistinct *ndistinct = NULL;
        MVDependencies *dependencies = NULL;
        MCVList *mcv = NULL;
        Datum exprstats = (Datum) 0;

        // Compute each requested type of statistic
        foreach(lc2, stat->types) {
            char t = (char) lfirst_int(lc2);

            switch (t) {
                case STATS_EXT_NDISTINCT:
                    ndistinct = statext_ndistinct_build(totalrows, data);
                    break;

                case STATS_EXT_DEPENDENCIES:
                    dependencies = statext_dependencies_build(data);
                    break;

                case STATS_EXT_MCV:
                    mcv = statext_mcv_build(data, totalrows, stattarget);
                    break;

                case STATS_EXT_EXPRESSIONS:
                    if (!stat->exprs)
                        elog(ERROR, "requested expression stats, but there are no expressions");

                    AnlExprData *exprdata = build_expr_data(stat->exprs, stattarget);
                    int nexprs = list_length(stat->exprs);

                    compute_expr_stats(onerel, totalrows, exprdata, nexprs, rows, numrows);
                    exprstats = serialize_expr_stats(exprdata, nexprs);
                    break;
            }
        }

        // Store computed statistics in catalog
        statext_store(stat->statOid, inh, ndistinct, dependencies, mcv, exprstats, stats);

        // Update progress reporting
        pgstat_progress_update_param(PROGRESS_ANALYZE_EXT_STATS_COMPUTED, ++ext_cnt);

        // Reset memory context for next statistics object
        MemoryContextReset(cxt);
    }

    // Clean up
    MemoryContextSwitchTo(oldcxt);
    MemoryContextDelete(cxt);
    list_free(statslist);
    table_close(pg_stext, RowExclusiveLock);
}
```