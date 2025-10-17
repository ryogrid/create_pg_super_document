# brincostestimate

## Location
[src/backend/utils/adt/selfuncs.c:8039-8254](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L8039-L8254)

## Overview
The  function estimates the cost of using a BRIN (Block Range Index) index for query execution, calculating startup costs, total costs, selectivity, correlation, and pages required for the PostgreSQL query planner.

## Definition

```c
void
brincostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
				 Cost *indexStartupCost, Cost *indexTotalCost,
				 Selectivity *indexSelectivity, double *indexCorrelation,
				 double *indexPages)
```
## Detailed Description
This function implements cost estimation specifically for BRIN indexes, which have fundamentally different search behavior compared to other PostgreSQL index types. BRIN indexes store summaries for ranges of pages rather than individual tuples, making their cost calculation unique.

The function performs several key operations:
1. **Statistics Gathering**: Obtains BRIN-specific statistics including pages per range and revmap pages
2. **Correlation Analysis**: Calculates index correlation by examining statistics for indexed columns
3. **Range Estimation**: Estimates the number of ranges that need to be scanned based on query selectivity and correlation
4. **Cost Calculation**: Computes startup and total costs considering sequential revmap reads and random page access patterns
5. **Selectivity Estimation**: Determines the portion of the table expected to be visited

The cost model accounts for BRIN's two-phase access pattern: first reading the reverse map (revmap) sequentially to identify relevant ranges, then potentially accessing regular index pages in random order.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing query planning context and statistics
- `*path`: IndexPath structure representing the specific index access path being costed
- `loop_count`: Number of times the index scan is expected to be executed (for nested loops)
- `*indexStartupCost`: Output parameter for the cost to start the index scan (revmap reading)
- `*indexTotalCost`: Output parameter for the total cost including all index operations
- `*indexSelectivity`: Output parameter for the estimated selectivity of the index scan
- `*indexCorrelation`: Output parameter for the correlation between index and table ordering
- `*indexPages`: Output parameter for the total number of index pages
## Dependencies
- Functions called/Symbols referenced:
  - [get_quals_from_indexclauses](../g/get_quals_from_indexclauses.md)
  - planner_rt_fetch
  - [get_tablespace_page_costs](../g/get_tablespace_page_costs.md)
  - [index_open](../i/index_open.md)
  - [brinGetStats](brinGetStats.md)
  - [index_close](../i/index_close.md)
  - [SearchSysCache3](../S/SearchSysCache3.md)
  - [get_attstatsslot](../g/get_attstatsslot.md)
  - [clauselist_selectivity](../c/clauselist_selectivity.md)
  - [index_other_operands_eval_cost](../i/index_other_operands_eval_cost.md)
  - CLAMP_PROBABILITY
- Called from (representative examples):
  - [brinhandler](brinhandler.md) (in src/backend/access/brin/brin.c:281)

## Notes and Other Information
- BRIN indexes have unique cost characteristics due to their range-based storage model
- The function handles both real and hypothetical indexes, using default values when actual statistics are unavailable
- Correlation plays a crucial role in BRIN cost estimation since it affects how many ranges need to be scanned
- The cost model includes a small charge (0.1 * cpu_operator_cost) per expected matching range to account for bitmap manipulation overhead
- The function assumes BRIN_DEFAULT_PAGES_PER_RANGE (128) pages per range for hypothetical indexes
- Cost calculation considers both sequential access for the revmap and random access for regular index pages

## Simplified Source

```c
void brincostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
                     Cost *indexStartupCost, Cost *indexTotalCost,
                     Selectivity *indexSelectivity, double *indexCorrelation,
                     double *indexPages) {
    IndexOptInfo *index = path->indexinfo;
    List *indexQuals = get_quals_from_indexclauses(path->indexclauses);
    RelOptInfo *baserel = index->rel;
    Cost spc_seq_page_cost, spc_random_page_cost;
    double qual_arg_cost;
    double qualSelectivity;
    BrinStatsData statsData;
    double indexRanges, minimalRanges, estimatedRanges, selec;

    // Get tablespace page costs
    get_tablespace_page_costs(index->reltablespace, &spc_random_page_cost, &spc_seq_page_cost);

    // Get BRIN statistics or use defaults for hypothetical indexes
    if (!index->hypothetical) {
        Relation indexRel = index_open(index->indexoid, NoLock);
        brinGetStats(indexRel, &statsData);
        index_close(indexRel, NoLock);
        indexRanges = Max(ceil((double) baserel->pages / statsData.pagesPerRange), 1.0);
    } else {
        indexRanges = Max(ceil((double) baserel->pages / BRIN_DEFAULT_PAGES_PER_RANGE), 1.0);
        statsData.pagesPerRange = BRIN_DEFAULT_PAGES_PER_RANGE;
        statsData.revmapNumPages = (indexRanges / REVMAP_PAGE_MAXITEMS) + 1;
    }

    // Compute index correlation from column statistics
    *indexCorrelation = 0;
    foreach(lc, path->indexclauses) {
        IndexClause *iclause = lfirst_node(IndexClause, lc);
        AttrNumber attnum = index->indexkeys[iclause->indexcol];

        // Get statistics and extract correlation if available
        if (attnum != 0) {
            /* lookup stats for base table column */
        } else {
            /* lookup stats for index expression column */
        }

        if (/* correlation found */) {
            *indexCorrelation = Max(*indexCorrelation, fabs(correlation_value));
        }
    }

    // Calculate selectivity and estimate ranges to scan
    qualSelectivity = clauselist_selectivity(root, indexQuals, baserel->relid, JOIN_INNER, NULL);
    minimalRanges = ceil(indexRanges * qualSelectivity);

    // Estimate actual ranges based on correlation
    if (*indexCorrelation < 1.0e-10)
        estimatedRanges = indexRanges;
    else
        estimatedRanges = Min(minimalRanges / *indexCorrelation, indexRanges);

    selec = estimatedRanges / indexRanges;
    CLAMP_PROBABILITY(selec);
    *indexSelectivity = selec;

    // Calculate costs
    qual_arg_cost = index_other_operands_eval_cost(root, indexQuals);

    // Startup cost: sequential read of revmap + qual evaluation
    *indexStartupCost = spc_seq_page_cost * statsData.revmapNumPages * loop_count + qual_arg_cost;

    // Total cost: startup + random access to regular index pages + bitmap manipulation
    *indexTotalCost = *indexStartupCost +
                     spc_random_page_cost * (index->pages - statsData.revmapNumPages) * loop_count +
                     0.1 * cpu_operator_cost * estimatedRanges * statsData.pagesPerRange;

    *indexPages = index->pages;
}
```

**Core Logic**: Calculates BRIN index costs by determining ranges to scan based on query selectivity and correlation, with startup costs for sequential revmap reading and total costs including random page access and bitmap manipulation overhead.