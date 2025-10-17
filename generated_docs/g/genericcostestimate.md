# genericcostestimate

## Location
[src/backend/utils/adt/selfuncs.c:6610-6832](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L6610-L6832)

## Overview
A general-purpose cost estimation function for index access paths that provides basic cost calculations for startup, total cost, selectivity, and correlation that can be used by specific index access methods.

## Definition

```c
void
genericcostestimate(PlannerInfo *root,
					IndexPath *path,
					double loop_count,
					GenericCosts *costs)
```
## Detailed Description
The  function provides a comprehensive cost model for index operations that serves as a foundation for more specialized index access method cost estimators. It calculates various cost components including disk I/O costs, CPU costs, and selectivity estimates.

The function performs several key calculations:
- Estimates the number of index tuples and pages that will be accessed
- Calculates disk I/O costs using the Mackert-Lohman formula to account for cache effects
- Computes CPU costs for evaluating index qualifiers and operators
- Handles ScalarArrayOpExpr operations that result in multiple index scans
- Applies partial index predicates to improve selectivity estimates

The cost model considers nested loop scenarios where the index scan may be repeated multiple times, applying cache-aware algorithms to estimate realistic I/O costs rather than assuming every page access results in a disk read.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing global planning context and statistics
- `*path`: IndexPath structure describing the specific index access path being costed
- `loop_count`: Expected number of times this index scan will be executed (for nested loops)
- `*costs`: GenericCosts output structure to store the calculated cost estimates
## Dependencies
- Functions called/Symbols referenced:
  - [get_quals_from_indexclauses](get_quals_from_indexclauses.md)
  - [add_predicate_to_index_quals](../a/add_predicate_to_index_quals.md)
  - [estimate_array_length](../e/estimate_array_length.md)
  - [clauselist_selectivity](../c/clauselist_selectivity.md)
  - [get_tablespace_page_costs](get_tablespace_page_costs.md)
  - [index_pages_fetched](../i/index_pages_fetched.md)
  - [index_other_operands_eval_cost](../i/index_other_operands_eval_cost.md)
  - lsecond
- Called from (representative examples):
  - [btcostestimate](../b/btcostestimate.md)
  - [hashcostestimate](../h/hashcostestimate.md)
  - [gistcostestimate](gistcostestimate.md)
  - [spgcostestimate](../s/spgcostestimate.md)

## Notes and Other Information
- Sets index correlation to 0.0 as a generic assumption, though specific index types may override this
- Handles both single scans and multiple scans from ScalarArrayOpExpr operations
- Uses the Mackert-Lohman formula for cache-aware I/O cost estimation when multiple scans are involved
- Estimates are primarily focused on leaf page access costs; upper tree level costs are left to specific index AM implementations
- The function provides a baseline that specific index access methods can build upon or override as needed

## Simplified Source

```c
void genericcostestimate(PlannerInfo *root, IndexPath *path, double loop_count, GenericCosts *costs)
{
    IndexOptInfo *index = path->indexinfo;
    List *indexQuals = get_quals_from_indexclauses(path->indexclauses);
    List *indexOrderBys = path->indexorderbys;

    Cost indexStartupCost;
    Cost indexTotalCost;
    Selectivity indexSelectivity;
    double numIndexPages;
    double numIndexTuples;
    double spc_random_page_cost;
    double num_sa_scans;
    double qual_op_cost;
    double qual_arg_cost;

    // Include index predicate if partial index
    List *selectivityQuals = add_predicate_to_index_quals(index, indexQuals);

    // Estimate ScalarArrayOpExpr scans if not provided
    num_sa_scans = costs->num_sa_scans;
    if (num_sa_scans < 1) {
        num_sa_scans = 1;
        // Calculate scan multiplier for array operations
        ListCell *l;
        foreach(l, indexQuals) {
            RestrictInfo *rinfo = (RestrictInfo *) lfirst(l);
            if (IsA(rinfo->clause, ScalarArrayOpExpr)) {
                ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) rinfo->clause;
                double alength = estimate_array_length(root, lsecond(saop->args));
                if (alength > 1)
                    num_sa_scans *= alength;
            }
        }
    }

    // Estimate selectivity and number of tuples
    indexSelectivity = clauselist_selectivity(root, selectivityQuals,
                                             index->rel->relid, JOIN_INNER, NULL);

    numIndexTuples = costs->numIndexTuples;
    if (numIndexTuples <= 0.0) {
        numIndexTuples = indexSelectivity * index->rel->tuples;
        numIndexTuples = rint(numIndexTuples / num_sa_scans);
    }

    // Bound tuple estimates
    if (numIndexTuples > index->tuples)
        numIndexTuples = index->tuples;
    if (numIndexTuples < 1.0)
        numIndexTuples = 1.0;

    // Estimate index pages to access
    if (index->pages > 1 && index->tuples > 1)
        numIndexPages = ceil(numIndexTuples * index->pages / index->tuples);
    else
        numIndexPages = 1.0;

    // Get storage cost parameters
    get_tablespace_page_costs(index->reltablespace, &spc_random_page_cost, NULL);

    // Calculate disk I/O costs
    double num_outer_scans = loop_count;
    double num_scans = num_sa_scans * num_outer_scans;

    if (num_scans > 1) {
        // Use cache-aware formula for multiple scans
        double pages_fetched = numIndexPages * num_scans;
        pages_fetched = index_pages_fetched(pages_fetched, index->pages,
                                          (double) index->pages, root);
        indexTotalCost = (pages_fetched * spc_random_page_cost) / num_outer_scans;
    } else {
        // Single scan: simple cost per page
        indexTotalCost = numIndexPages * spc_random_page_cost;
    }

    // Calculate CPU costs for qualifier evaluation
    qual_arg_cost = index_other_operands_eval_cost(root, indexQuals) +
                   index_other_operands_eval_cost(root, indexOrderBys);
    qual_op_cost = cpu_operator_cost *
                  (list_length(indexQuals) + list_length(indexOrderBys));

    indexStartupCost = qual_arg_cost;
    indexTotalCost += qual_arg_cost;
    indexTotalCost += numIndexTuples * num_sa_scans * (cpu_index_tuple_cost + qual_op_cost);

    // Return all cost estimates
    costs->indexStartupCost = indexStartupCost;
    costs->indexTotalCost = indexTotalCost;
    costs->indexSelectivity = indexSelectivity;
    costs->indexCorrelation = 0.0;  // Generic assumption
    costs->numIndexPages = numIndexPages;
    costs->numIndexTuples = numIndexTuples;
    costs->spc_random_page_cost = spc_random_page_cost;
    costs->num_sa_scans = num_sa_scans;
}
```