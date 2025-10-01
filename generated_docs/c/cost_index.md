# cost_index

## Location
[src/backend/optimizer/path/costsize.c:549-839](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L549-L839)

## Overview
Calculates the comprehensive cost estimate for scanning a relation using an index, including both index access costs and heap tuple retrieval costs.

## Definition
```c
void cost_index(IndexPath *path, PlannerInfo *root, double loop_count,
                bool partial_path)
```

## Detailed Description
The `cost_index` function determines the total cost of executing an index scan operation, which is one of the most complex costing functions in PostgreSQL's optimizer. It calculates costs for both accessing the index itself and fetching the corresponding heap tuples. The function uses access method-specific cost estimation via the index's amcostestimate function, then applies sophisticated models for heap page access costs based on index correlation with heap order.

The function handles multiple scenarios including regular index scans, index-only scans, parameterized paths, and parallel execution. It uses the Mackert and Lohman formula for uncorrelated access patterns and interpolates between random and sequential costs based on index-heap correlation. For index-only scans, it accounts for visibility map information to reduce estimated heap page fetches.

## Parameters / Member Variables
- `path`: The IndexPath structure to populate with cost estimates and configuration
- `root`: PlannerInfo containing global planning context and configuration  
- `loop_count`: Number of repetitions of the indexscan for caching behavior estimation
- `partial_path`: Boolean indicating whether this is a partial path for parallel execution

## Dependencies
- Functions called/Symbols referenced:
  - [IndexPath](../I/IndexPath.md) (structure)
  - [IndexOptInfo](../I/IndexOptInfo.md) (structure)
  - Cost (type)
  - [QualCost](../Q/QualCost.md) (structure)
  - [list_concat](../l/list_concat.md)
  - [extract_nonindex_conditions](../e/extract_nonindex_conditions.md)
  - [clamp_row_est](clamp_row_est.md)
  - [get_tablespace_page_costs](../g/get_tablespace_page_costs.md)
  - [index_pages_fetched](../i/index_pages_fetched.md)
  - [compute_parallel_worker](compute_parallel_worker.md)
  - [cost_qual_eval](cost_qual_eval.md)
  - [get_parallel_divisor](../g/get_parallel_divisor.md)
  - RTE_RELATION (constant)
- Called from (representative examples):
  - [create_index_path](create_index_path.md)
  - [reparameterize_path](../r/reparameterize_path.md)

## Notes and Other Information
This function implements one of the most sophisticated costing models in PostgreSQL, handling the complex relationship between index selectivity, heap page correlation, and access patterns. It distinguishes between startup costs (index initialization and qualification setup) and run costs (per-tuple processing and I/O). The correlation-based interpolation between random and sequential access costs is crucial for accurate cost estimation, especially for clustered tables. For parallel execution, it adjusts both worker count estimation and cost distribution among workers.

## Simplified Source

```c
void
cost_index(IndexPath *path, PlannerInfo *root, double loop_count,
           bool partial_path)
{
    IndexOptInfo *index = path->indexinfo;
    RelOptInfo *baserel = index->rel;
    bool indexonly = (path->path.pathtype == T_IndexOnlyScan);
    amcostestimate_function amcostestimate;
    List *qpquals;
    Cost startup_cost = 0;
    Cost run_cost = 0;
    Cost cpu_run_cost = 0;
    Cost indexStartupCost;
    Cost indexTotalCost;
    Selectivity indexSelectivity;
    double indexCorrelation, csquared;
    double spc_seq_page_cost, spc_random_page_cost;
    Cost min_IO_cost, max_IO_cost;
    QualCost qpqual_cost;
    Cost cpu_per_tuple;
    double tuples_fetched;
    double pages_fetched;
    double rand_heap_pages;
    double index_pages;

    // Set row estimates and extract non-index conditions (qpquals)
    if (path->path.param_info)
    {
        path->path.rows = path->path.param_info->ppi_rows;
        qpquals = list_concat(
            extract_nonindex_conditions(path->indexinfo->indrestrictinfo, path->indexclauses),
            extract_nonindex_conditions(path->path.param_info->ppi_clauses, path->indexclauses));
    }
    else
    {
        path->path.rows = baserel->rows;
        qpquals = extract_nonindex_conditions(path->indexinfo->indrestrictinfo, path->indexclauses);
    }

    if (!enable_indexscan)
        startup_cost += disable_cost;

    // Get index-specific cost estimates
    amcostestimate = (amcostestimate_function) index->amcostestimate;
    amcostestimate(root, path, loop_count,
                   &indexStartupCost, &indexTotalCost,
                   &indexSelectivity, &indexCorrelation,
                   &index_pages);

    // Store for potential bitmap scan use
    path->indextotalcost = indexTotalCost;
    path->indexselectivity = indexSelectivity;

    // Add index access costs
    startup_cost += indexStartupCost;
    run_cost += indexTotalCost - indexStartupCost;

    // Estimate heap tuples to fetch
    tuples_fetched = clamp_row_est(indexSelectivity * baserel->tuples);

    // Get tablespace page costs
    get_tablespace_page_costs(baserel->reltablespace,
                              &spc_random_page_cost, &spc_seq_page_cost);

    // Calculate heap page I/O costs
    if (loop_count > 1)
    {
        // Multi-scan case: scale up for caching effects
        pages_fetched = index_pages_fetched(tuples_fetched * loop_count,
                                            baserel->pages, (double) index->pages, root);
        if (indexonly)
            pages_fetched = ceil(pages_fetched * (1.0 - baserel->allvisfrac));

        rand_heap_pages = pages_fetched;
        max_IO_cost = (pages_fetched * spc_random_page_cost) / loop_count;

        // Correlated case for multi-scan
        pages_fetched = ceil(indexSelectivity * (double) baserel->pages);
        pages_fetched = index_pages_fetched(pages_fetched * loop_count,
                                            baserel->pages, (double) index->pages, root);
        if (indexonly)
            pages_fetched = ceil(pages_fetched * (1.0 - baserel->allvisfrac));

        min_IO_cost = (pages_fetched * spc_random_page_cost) / loop_count;
    }
    else
    {
        // Single scan case: use Mackert & Lohman formula
        pages_fetched = index_pages_fetched(tuples_fetched,
                                            baserel->pages, (double) index->pages, root);
        if (indexonly)
            pages_fetched = ceil(pages_fetched * (1.0 - baserel->allvisfrac));

        rand_heap_pages = pages_fetched;
        max_IO_cost = pages_fetched * spc_random_page_cost;

        // Perfectly correlated case
        pages_fetched = ceil(indexSelectivity * (double) baserel->pages);
        if (indexonly)
            pages_fetched = ceil(pages_fetched * (1.0 - baserel->allvisfrac));

        if (pages_fetched > 0)
        {
            min_IO_cost = spc_random_page_cost;
            if (pages_fetched > 1)
                min_IO_cost += (pages_fetched - 1) * spc_seq_page_cost;
        }
        else
            min_IO_cost = 0;
    }

    // Handle parallel path setup
    if (partial_path)
    {
        if (indexonly)
            rand_heap_pages = -1;

        path->path.parallel_workers = compute_parallel_worker(baserel,
                                                              rand_heap_pages, index_pages,
                                                              max_parallel_workers_per_gather);
        if (path->path.parallel_workers <= 0)
            return;

        path->path.parallel_aware = true;
    }

    // Interpolate I/O cost based on correlation
    csquared = indexCorrelation * indexCorrelation;
    run_cost += max_IO_cost + csquared * (min_IO_cost - max_IO_cost);

    // Add CPU costs for tuple processing and qualification
    cost_qual_eval(&qpqual_cost, qpquals, root);
    startup_cost += qpqual_cost.startup;
    cpu_per_tuple = cpu_tuple_cost + qpqual_cost.per_tuple;
    cpu_run_cost += cpu_per_tuple * tuples_fetched;

    // Add target list evaluation costs
    startup_cost += path->path.pathtarget->cost.startup;
    cpu_run_cost += path->path.pathtarget->cost.per_tuple * path->path.rows;

    // Adjust for parallelism
    if (path->path.parallel_workers > 0)
    {
        double parallel_divisor = get_parallel_divisor(&path->path);
        path->path.rows = clamp_row_est(path->path.rows / parallel_divisor);
        cpu_run_cost /= parallel_divisor;
    }

    run_cost += cpu_run_cost;

    // Set final costs
    path->path.startup_cost = startup_cost;
    path->path.total_cost = startup_cost + run_cost;
}
```