# do_analyze_rel

## Location
[src/backend/commands/analyze.c:280-827](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/analyze.c#L280-L827)

## Overview
The core function that performs the actual statistics analysis for a relation, handling both regular and inherited (recursive) analysis modes.

## Definition

```c
structure is allocated in anl_context.
	 */
	if (numrows > 0)
	{
		MemoryContext col_context,
					old_context;

		pgstat_progress_update_param(PROGRESS_ANALYZE_PHASE,
									 PROGRESS_ANALYZE_PHASE_COMPUTE_STATS);

		col_context = AllocSetContextCreate(anl_context,
											"Analyze Column",
											ALLOCSET_DEFAULT_SIZES);
		old_context = MemoryContextSwitchTo(col_context);

		for (i = 0; i < attr_cnt; i++)
		{
			VacAttrStats *stats = vacattrstats[i];
			AttributeOpts *aopt;

			stats->rows = rows;
			stats->tupDesc = onerel->rd_att;
			stats->compute_stats(stats,
								 std_fetch_func,
								 numrows,
								 totalrows);

			/*
			 * If the appropriate flavor of the n_distinct option is
			 * specified, override with the corresponding value.
			 */
			aopt = get_attribute_options(onerel->rd_id, stats->tupattnum);
			if (aopt != NULL)
			{
				float8		n_distinct;

				n_distinct = inh ? aopt->n_distinct_inherited : aopt->n_distinct;
				if (n_distinct != 0.0)
					stats->stadistinct = n_distinct;
			}

			MemoryContextReset(col_context);
		}

		if (nindexes > 0)
			compute_index_stats(onerel, totalrows,
								indexdata, nindexes,
								rows, numrows,
								col_context);

		MemoryContextSwitchTo(old_context);
		MemoryContextDelete(col_context);

		/*
		 * Emit the completed stats rows into pg_statistic, replacing any
		 * previous statistics for the target columns.  (If there are stats in
		 * pg_statistic for columns we didn't process, we leave them alone.)
		 */
		update_attstats(RelationGetRelid(onerel), inh,
						attr_cnt, vacattrstats);

		for (ind = 0; ind < nindexes; ind++)
		{
			AnlIndexData *thisdata = &indexdata[ind];

			update_attstats(RelationGetRelid(Irel[ind]), false,
							thisdata->attr_cnt, thisdata->vacattrstats);
		}

		/* Build extended statistics (if there are any). */
		BuildRelationExtStatistics(onerel, inh, totalrows, numrows, rows,
								   attr_cnt, vacattrstats);
	}

	pgstat_progress_update_param(PROGRESS_ANALYZE_PHASE,
								 PROGRESS_ANALYZE_PHASE_FINALIZE_ANALYZE);
```
## Detailed Description
This function orchestrates the complete analysis process for a relation, including column selection, index analysis, row sampling, statistics computation, and metadata updates. It operates in two modes: regular analysis for individual tables and inherited analysis for partitioned tables that processes all child partitions. The function sets up proper security contexts, manages memory allocation, samples rows using the provided acquisition function, computes statistics for both table columns and index expressions, and updates the system catalogs with the results.

Key phases include: determining which columns to analyze, opening and examining indexes for analyzable expressions, calculating required sample size, acquiring sample rows, computing column and index statistics, updating pg_statistic and pg_class catalogs, building extended statistics, and performing cleanup operations.

## Parameters

- `onerel`: The relation being analyzed
- `params`: Vacuum parameters containing analysis options and configuration
- `va_cols`: List of specific columns to analyze (NIL for all columns)
- `acquirefunc`: Function pointer for acquiring sample rows
- `relpages`: Number of pages in the relation
- `inh`: Boolean indicating inherited/recursive analysis mode
- `in_outer_xact`: Boolean indicating if running within an outer transaction
- `elevel`: Error level for logging messages

## Dependencies
- Functions called/Symbols referenced:
  - : Analyzes individual columns and creates VacAttrStats
  - : Computes statistics for index expressions
  - : Samples rows from partitioned tables
  - : Updates pg_statistic with computed statistics
  - : Creates extended statistics objects
  - : Updates pg_class with relation statistics
  - : Counts all-visible pages for storage relations
  - : Reports analysis completion to stats collector
- Called from (representative examples):
  - : Main analyze entry point (both regular and recursive calls)

## Notes and Other Information
- Creates a dedicated memory context 'Analyze' for temporary allocations during analysis
- Switches to table owner's user ID and restricts security operations during index function execution
- Supports analysis of regular tables, materialized views, foreign tables, and partitioned tables
- Handles index expressions analysis when no explicit column list is provided
- Uses progress reporting to track analysis phases for monitoring tools
- Performs extensive logging for autovacuum operations including I/O timing and buffer usage statistics
- Updates both table and index statistics in pg_class, but only for non-inherited analysis
- Calls index cleanup routines for ANALYZE-only operations (excluding VACUUM ANALYZE)

## Simplified Source

```c
static void
do_analyze_rel(Relation onerel, VacuumParams *params,
               List *va_cols, AcquireSampleRowsFunc acquirefunc,
               BlockNumber relpages, bool inh, bool in_outer_xact,
               int elevel)
{
    int attr_cnt, i, ind;
    Relation *Irel;
    int nindexes;
    bool hasindex;
    VacAttrStats **vacattrstats;
    AnlIndexData *indexdata;
    int targrows, numrows;
    double totalrows, totaldeadrows;
    HeapTuple *rows;
    MemoryContext caller_context;
    Oid save_userid;
    int save_sec_context, save_nestlevel;

    // Log what we're analyzing
    if (inh)
        ereport(elevel, (errmsg("analyzing \"%s.%s\" inheritance tree",
                               get_namespace_name(RelationGetNamespace(onerel)),
                               RelationGetRelationName(onerel))));
    else
        ereport(elevel, (errmsg("analyzing \"%s.%s\"",
                               get_namespace_name(RelationGetNamespace(onerel)),
                               RelationGetRelationName(onerel))));

    // Setup memory context and security restrictions
    anl_context = AllocSetContextCreate(CurrentMemoryContext,
                                       "Analyze", ALLOCSET_DEFAULT_SIZES);
    caller_context = MemoryContextSwitchTo(anl_context);

    GetUserIdAndSecContext(&save_userid, &save_sec_context);
    SetUserIdAndSecContext(onerel->rd_rel->relowner,
                          save_sec_context | SECURITY_RESTRICTED_OPERATION);
    save_nestlevel = NewGUCNestLevel();
    RestrictSearchPath();

    // Determine which columns to analyze
    if (va_cols != NIL) {
        // Analyze specific columns from command
        vacattrstats = (VacAttrStats **) palloc(list_length(va_cols) *
                                               sizeof(VacAttrStats *));
        attr_cnt = 0;
        foreach(ListCell *le, va_cols) {
            char *col = strVal(lfirst(le));
            i = attnameAttNum(onerel, col, false);

            if (i == InvalidAttrNumber)
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
                               errmsg("column \"%s\" does not exist", col)));

            vacattrstats[attr_cnt] = examine_attribute(onerel, i, NULL);
            if (vacattrstats[attr_cnt] != NULL)
                attr_cnt++;
        }
    } else {
        // Analyze all columns
        attr_cnt = onerel->rd_att->natts;
        vacattrstats = (VacAttrStats **) palloc(attr_cnt * sizeof(VacAttrStats *));
        int tcnt = 0;
        for (i = 1; i <= attr_cnt; i++) {
            vacattrstats[tcnt] = examine_attribute(onerel, i, NULL);
            if (vacattrstats[tcnt] != NULL)
                tcnt++;
        }
        attr_cnt = tcnt;
    }

    // Handle indexes (skip for partitioned tables and inherited analysis)
    if (onerel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE) {
        List *idxs = RelationGetIndexList(onerel);
        hasindex = idxs != NIL;
        list_free(idxs);
        Irel = NULL;
        nindexes = 0;
    } else if (!inh) {
        vac_open_indexes(onerel, AccessShareLock, &nindexes, &Irel);
        hasindex = nindexes > 0;
    } else {
        Irel = NULL;
        nindexes = 0;
        hasindex = false;
    }

    // Setup index analysis if needed
    indexdata = NULL;
    if (nindexes > 0) {
        indexdata = (AnlIndexData *) palloc0(nindexes * sizeof(AnlIndexData));
        for (ind = 0; ind < nindexes; ind++) {
            AnlIndexData *thisdata = &indexdata[ind];
            IndexInfo *indexInfo = BuildIndexInfo(Irel[ind]);
            thisdata->indexInfo = indexInfo;
            thisdata->tupleFract = 1.0;

            // Handle index expressions
            if (indexInfo->ii_Expressions != NIL && va_cols == NIL) {
                thisdata->vacattrstats = (VacAttrStats **)
                    palloc(indexInfo->ii_NumIndexAttrs * sizeof(VacAttrStats *));
                int tcnt = 0;
                ListCell *indexpr_item = list_head(indexInfo->ii_Expressions);

                for (i = 0; i < indexInfo->ii_NumIndexAttrs; i++) {
                    if (indexInfo->ii_IndexAttrNumbers[i] == 0) {
                        Node *indexkey = (Node *) lfirst(indexpr_item);
                        indexpr_item = lnext(indexInfo->ii_Expressions, indexpr_item);
                        thisdata->vacattrstats[tcnt] =
                            examine_attribute(Irel[ind], i + 1, indexkey);
                        if (thisdata->vacattrstats[tcnt] != NULL)
                            tcnt++;
                    }
                }
                thisdata->attr_cnt = tcnt;
            }
        }
    }

    // Calculate target sample size
    targrows = 100;  // minimum sample size
    for (i = 0; i < attr_cnt; i++) {
        if (targrows < vacattrstats[i]->minrows)
            targrows = vacattrstats[i]->minrows;
    }
    for (ind = 0; ind < nindexes; ind++) {
        for (i = 0; i < indexdata[ind].attr_cnt; i++) {
            if (targrows < indexdata[ind].vacattrstats[i]->minrows)
                targrows = indexdata[ind].vacattrstats[i]->minrows;
        }
    }

    // Consider extended statistics requirements
    int minrows = ComputeExtStatisticsRows(onerel, attr_cnt, vacattrstats);
    if (targrows < minrows)
        targrows = minrows;

    // Acquire sample rows
    rows = (HeapTuple *) palloc(targrows * sizeof(HeapTuple));
    if (inh)
        numrows = acquire_inherited_sample_rows(onerel, elevel,
                                              rows, targrows,
                                              &totalrows, &totaldeadrows);
    else
        numrows = (*acquirefunc)(onerel, elevel, rows, targrows,
                                &totalrows, &totaldeadrows);

    // Compute statistics if we have sample data
    if (numrows > 0) {
        MemoryContext col_context = AllocSetContextCreate(anl_context,
                                                         "Analyze Column",
                                                         ALLOCSET_DEFAULT_SIZES);
        MemoryContext old_context = MemoryContextSwitchTo(col_context);

        // Compute column statistics
        for (i = 0; i < attr_cnt; i++) {
            VacAttrStats *stats = vacattrstats[i];
            stats->rows = rows;
            stats->tupDesc = onerel->rd_att;
            stats->compute_stats(stats, std_fetch_func, numrows, totalrows);

            // Apply n_distinct override if configured
            AttributeOpts *aopt = get_attribute_options(onerel->rd_id, stats->tupattnum);
            if (aopt != NULL) {
                float8 n_distinct = inh ? aopt->n_distinct_inherited : aopt->n_distinct;
                if (n_distinct != 0.0)
                    stats->stadistinct = n_distinct;
            }
            MemoryContextReset(col_context);
        }

        // Compute index statistics
        if (nindexes > 0)
            compute_index_stats(onerel, totalrows, indexdata, nindexes,
                              rows, numrows, col_context);

        MemoryContextSwitchTo(old_context);
        MemoryContextDelete(col_context);

        // Update pg_statistic with computed statistics
        update_attstats(RelationGetRelid(onerel), inh, attr_cnt, vacattrstats);
        for (ind = 0; ind < nindexes; ind++) {
            update_attstats(RelationGetRelid(Irel[ind]), false,
                           indexdata[ind].attr_cnt, indexdata[ind].vacattrstats);
        }

        // Build extended statistics
        BuildRelationExtStatistics(onerel, inh, totalrows, numrows, rows,
                                 attr_cnt, vacattrstats);
    }

    // Update pg_class statistics (not for inherited analysis)
    if (!inh) {
        BlockNumber relallvisible = 0;
        if (RELKIND_HAS_STORAGE(onerel->rd_rel->relkind))
            visibilitymap_count(onerel, &relallvisible, NULL);

        CommandCounterIncrement();
        vac_update_relstats(onerel, relpages, totalrows, relallvisible,
                           hasindex, InvalidTransactionId, InvalidMultiXactId,
                           NULL, NULL, in_outer_xact);

        // Update index statistics too
        for (ind = 0; ind < nindexes; ind++) {
            double totalindexrows = ceil(indexdata[ind].tupleFract * totalrows);
            vac_update_relstats(Irel[ind],
                               RelationGetNumberOfBlocks(Irel[ind]),
                               totalindexrows, 0, false,
                               InvalidTransactionId, InvalidMultiXactId,
                               NULL, NULL, in_outer_xact);
        }
    } else if (onerel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE) {
        // Update partitioned table statistics
        CommandCounterIncrement();
        vac_update_relstats(onerel, -1, totalrows, 0, hasindex,
                           InvalidTransactionId, InvalidMultiXactId,
                           NULL, NULL, in_outer_xact);
    }

    // Report to stats collector
    if (!inh)
        pgstat_report_analyze(onerel, totalrows, totaldeadrows, (va_cols == NIL));
    else if (onerel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
        pgstat_report_analyze(onerel, 0, 0, (va_cols == NIL));

    // Index cleanup for ANALYZE-only operations
    if (!(params->options & VACOPT_VACUUM)) {
        for (ind = 0; ind < nindexes; ind++) {
            IndexVacuumInfo ivinfo;
            ivinfo.index = Irel[ind];
            ivinfo.heaprel = onerel;
            ivinfo.analyze_only = true;
            ivinfo.estimated_count = true;
            ivinfo.message_level = elevel;
            ivinfo.num_heap_tuples = onerel->rd_rel->reltuples;
            ivinfo.strategy = vac_strategy;

            IndexBulkDeleteResult *stats = index_vacuum_cleanup(&ivinfo, NULL);
            if (stats)
                pfree(stats);
        }
    }

    // Cleanup
    vac_close_indexes(nindexes, Irel, NoLock);
    AtEOXact_GUC(false, save_nestlevel);
    SetUserIdAndSecContext(save_userid, save_sec_context);
    MemoryContextSwitchTo(caller_context);
    MemoryContextDelete(anl_context);
    anl_context = NULL;
}
```