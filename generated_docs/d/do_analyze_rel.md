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

## Parameters / Member Variables
- : The relation being analyzed
- : Vacuum parameters containing analysis options and configuration
- : List of specific columns to analyze (NIL for all columns)
- : Function pointer for acquiring sample rows
- : Number of pages in the relation
- : Boolean indicating inherited/recursive analysis mode
- : Boolean indicating if running within an outer transaction
- : Error level for logging messages

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