# gincostestimate

## Location
[src/backend/utils/adt/selfuncs.c:7649-8038](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L7649-L8038)

## Overview
Main cost estimation function for GIN (Generalized Inverted Index) access paths in PostgreSQL's query planner.

## Definition
void gincostestimate(PlannerInfo *root, IndexPath *path, double loop_count, Cost *indexStartupCost, Cost *indexTotalCost, Selectivity *indexSelectivity, double *indexCorrelation, double *indexPages)

## Detailed Description
The gincostestimate function provides comprehensive cost estimation for GIN index scans, which have fundamentally different search behavior compared to other index types. It retrieves statistical information from the index's meta page, analyzes each index clause to determine search patterns, and calculates costs based on the unique structure of GIN indexes. The function handles entry pages (containing the search keys), data pages (containing tuple pointers), and pending pages (from recent insertions). It estimates costs for tree descent, page fetches, partial matches, and accounts for ScalarArrayOp expressions that generate multiple scans. The cost model considers cache effects, random page access costs, and CPU costs for processing search entries and result tuples.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planning context and statistics
- `path`: IndexPath representing the specific GIN index access path being costed
- `loop_count`: Expected number of times this index scan will be executed in a nestloop
- `indexStartupCost`: Output parameter for one-time startup cost of the index scan
- `indexTotalCost`: Output parameter for total cost including startup and per-tuple costs
- `indexSelectivity`: Output parameter for estimated selectivity of the index condition
- `indexCorrelation`: Output parameter for correlation between index and heap order (always 0.0 for GIN)
- `indexPages`: Output parameter for estimated number of data pages to be accessed

## Dependencies
- Functions called/Symbols referenced:
  - [get_quals_from_indexclauses](get_quals_from_indexclauses.md)
  - [index_open](../i/index_open.md)
  - [ginGetStats](ginGetStats.md)
  - [index_close](../i/index_close.md)
  - [add_predicate_to_index_quals](../a/add_predicate_to_index_quals.md)
  - [clauselist_selectivity](../c/clauselist_selectivity.md)
  - [get_tablespace_page_costs](get_tablespace_page_costs.md)
  - [gincost_opexpr](gincost_opexpr.md)
  - [gincost_scalararrayopexpr](gincost_scalararrayopexpr.md)
  - [index_pages_fetched](../i/index_pages_fetched.md)
  - [index_other_operands_eval_cost](../i/index_other_operands_eval_cost.md)
  - GinStatsData
  - GinQualCounts
- Called from (representative examples):
  - [ginhandler](ginhandler.md)

## Notes and Other Information
- Retrieves actual statistics from GIN index meta page when available, falls back to heuristic estimates for hypothetical indexes
- Scales statistics based on index growth since last VACUUM, with fallback heuristics for excessive growth
- Assumes 90% entry pages, 10% data pages, and 100 entries per entry page when statistics are unavailable
- Handles full index scan cases where certain search modes require scanning all entries
- Models entry tree descent costs using logarithmic complexity similar to B-tree indexes
- Uses power function (numEntryPages^0.15) to estimate entry pages fetched during searches
- Accounts for partial match algorithm costs which require scanning leaf entry pages
- Applies cache effects modeling for multiple scans due to nestloops or array operations
- Uses random page cost since logically close pages may be physically distant on disk
- Includes cross-check based on overall selectivity to avoid under-estimation with high key frequency
- Always sets indexCorrelation to 0.0 since GIN indexes don't maintain tuple order correlation

## Simplified Source

```c
void gincostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
                    Cost *indexStartupCost, Cost *indexTotalCost,
                    Selectivity *indexSelectivity, double *indexCorrelation,
                    double *indexPages) {
    IndexOptInfo *index = path->indexinfo;
    List *indexQuals = get_quals_from_indexclauses(path->indexclauses);
    double numPages = index->pages, numTuples = index->tuples;
    double numEntryPages, numDataPages, numPendingPages, numEntries;
    GinQualCounts counts;
    bool matchPossible = true, fullIndexScan = false;
    double partialScale, entryPagesFetched, dataPagesFetched;
    double qual_op_cost, qual_arg_cost, spc_random_page_cost, outer_scans;
    Cost descentCost;
    GinStatsData ginStats;

    // Get GIN statistics or use defaults
    if (!index->hypothetical) {
        Relation indexRel = index_open(index->indexoid, NoLock);
        ginGetStats(indexRel, &ginStats);
        index_close(indexRel, NoLock);
    } else {
        memset(&ginStats, 0, sizeof(ginStats));
    }

    // Calculate index structure sizes
    numPendingPages = (ginStats.nPendingPages < numPages) ? ginStats.nPendingPages : 0;

    if (numPages > 0 && ginStats.nTotalPages <= numPages &&
        ginStats.nTotalPages > numPages / 4 && ginStats.nEntryPages > 0 && ginStats.nEntries > 0) {
        // Scale existing statistics
        double scale = numPages / ginStats.nTotalPages;
        numEntryPages = ceil(ginStats.nEntryPages * scale);
        numDataPages = ceil(ginStats.nDataPages * scale);
        numEntries = ceil(ginStats.nEntries * scale);
    } else {
        // Use heuristic estimates: 90% entry pages, 10% data pages
        numPages = Max(numPages, 10);
        numEntryPages = floor((numPages - numPendingPages) * 0.90);
        numDataPages = numPages - numPendingPages - numEntryPages;
        numEntries = floor(numEntryPages * 100);
    }

    numEntries = Max(numEntries, 1);

    // Calculate selectivity
    List *selectivityQuals = add_predicate_to_index_quals(index, indexQuals);
    *indexSelectivity = clauselist_selectivity(root, selectivityQuals, index->rel->relid, JOIN_INNER, NULL);
    get_tablespace_page_costs(index->reltablespace, &spc_random_page_cost, NULL);
    *indexCorrelation = 0.0;

    // Analyze index clauses to count search entries
    memset(&counts, 0, sizeof(counts));
    counts.arrayScans = 1;

    foreach(lc, path->indexclauses) {
        IndexClause *iclause = lfirst_node(IndexClause, lc);
        foreach(lc2, iclause->indexquals) {
            RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc2);
            Expr *clause = rinfo->clause;

            if (IsA(clause, OpExpr)) {
                matchPossible = gincost_opexpr(root, index, iclause->indexcol, (OpExpr *) clause, &counts);
            } else if (IsA(clause, ScalarArrayOpExpr)) {
                matchPossible = gincost_scalararrayopexpr(root, index, iclause->indexcol,
                                                         (ScalarArrayOpExpr *) clause, numEntries, &counts);
            }
            if (!matchPossible) break;
        }
    }

    if (!matchPossible) {
        *indexStartupCost = *indexTotalCost = *indexSelectivity = 0;
        return;
    }

    // Check for full index scan requirement
    for (int i = 0; i < index->nkeycolumns; i++) {
        if (counts.attHasFullScan[i] && !counts.attHasNormalScan[i]) {
            fullIndexScan = true;
            break;
        }
    }

    if (fullIndexScan || indexQuals == NIL) {
        counts.partialEntries = 0;
        counts.exactEntries = numEntries;
        counts.searchEntries = numEntries;
    }

    outer_scans = loop_count;

    // Calculate page access costs
    entryPagesFetched = numPendingPages + ceil(counts.searchEntries * rint(pow(numEntryPages, 0.15)));
    partialScale = Min(counts.partialEntries / numEntries, 1.0);
    entryPagesFetched += ceil(numEntryPages * partialScale);
    dataPagesFetched = ceil(numDataPages * partialScale);

    *indexStartupCost = *indexTotalCost = 0;

    // Add descent costs (logarithmic tree navigation)
    if (numEntries > 1) {
        descentCost = ceil(log(numEntries) / log(2.0)) * cpu_operator_cost;
        *indexStartupCost += descentCost * counts.searchEntries;
        *indexTotalCost += counts.arrayScans * descentCost * counts.searchEntries;
    }

    // Add page processing costs
    *indexStartupCost += entryPagesFetched * DEFAULT_PAGE_CPU_MULTIPLIER * cpu_operator_cost;
    *indexTotalCost += entryPagesFetched * counts.arrayScans * DEFAULT_PAGE_CPU_MULTIPLIER * cpu_operator_cost;
    *indexStartupCost += DEFAULT_PAGE_CPU_MULTIPLIER * cpu_operator_cost * dataPagesFetched;
    *indexTotalCost += dataPagesFetched * (counts.arrayScans - 1) * DEFAULT_PAGE_CPU_MULTIPLIER * cpu_operator_cost;

    // Apply cache effects for multiple scans
    if (outer_scans > 1 || counts.arrayScans > 1) {
        entryPagesFetched = index_pages_fetched(entryPagesFetched * outer_scans * counts.arrayScans,
                                               (BlockNumber) numEntryPages, numEntryPages, root) / outer_scans;
        dataPagesFetched = index_pages_fetched(dataPagesFetched * outer_scans * counts.arrayScans,
                                              (BlockNumber) numDataPages, numDataPages, root) / outer_scans;
    }

    // Add I/O costs
    *indexStartupCost += (entryPagesFetched + dataPagesFetched) * spc_random_page_cost;

    // Calculate final data page access costs
    dataPagesFetched = ceil(numDataPages * counts.exactEntries / numEntries);
    double dataPagesFetchedBySel = ceil(*indexSelectivity * (numTuples / (BLCKSZ / 3)));
    dataPagesFetched = Max(dataPagesFetched, dataPagesFetchedBySel);

    *indexStartupCost += DEFAULT_PAGE_CPU_MULTIPLIER * cpu_operator_cost * counts.searchEntries;
    *indexTotalCost += dataPagesFetched * counts.arrayScans * DEFAULT_PAGE_CPU_MULTIPLIER * cpu_operator_cost;

    if (outer_scans > 1 || counts.arrayScans > 1) {
        dataPagesFetched = index_pages_fetched(dataPagesFetched * outer_scans * counts.arrayScans,
                                              (BlockNumber) numDataPages, numDataPages, root) / outer_scans;
    }

    *indexTotalCost += *indexStartupCost + dataPagesFetched * spc_random_page_cost;

    // Add operator evaluation costs
    qual_arg_cost = index_other_operands_eval_cost(root, indexQuals);
    qual_op_cost = cpu_operator_cost * list_length(indexQuals);
    *indexStartupCost += qual_arg_cost;
    *indexTotalCost += qual_arg_cost + (counts.searchEntries * counts.arrayScans) * qual_op_cost +
                      (numTuples * *indexSelectivity) * cpu_index_tuple_cost;
    *indexPages = dataPagesFetched;
}
```

**Core Logic**: Comprehensive GIN cost estimation that analyzes index structure, processes search clauses through helper functions, calculates entry/data page access patterns, models tree descent and partial match costs, applies cache effects, and includes operator evaluation overhead.