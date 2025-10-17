# btcostestimate

## Location
[src/backend/utils/adt/selfuncs.c:6854-7196](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L6854-L7196)

## Overview
A specialized cost estimation function for B-tree index access paths that provides accurate cost calculations considering B-tree specific optimizations like index ordering correlation and boundary qualification analysis.

## Definition

```c
void
btcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
			   Cost *indexStartupCost, Cost *indexTotalCost,
			   Selectivity *indexSelectivity, double *indexCorrelation,
			   double *indexPages)
```
## Detailed Description
The  function provides specialized cost estimation for B-tree index scans, building upon the generic cost estimation framework while adding B-tree specific optimizations and considerations.

Key features include:
- **Boundary Qualification Analysis**: Identifies which index qualifiers actually determine scan boundaries (leading equality clauses plus the first inequality clause) versus those that only provide heap filtering
- **Unique Index Optimization**: For unique indexes with complete equality qualifiers, assumes exactly one tuple will be found
- **ScalarArrayOpExpr Handling**: Estimates the number of index descents for array operations and applies intelligent clamping to avoid unrealistic estimates
- **Index Correlation Calculation**: Uses statistics from the first indexed column to estimate how well the index ordering matches the table's physical ordering
- **B-tree Descent Costing**: Adds CPU costs for traversing the B-tree from root to leaf, accounting for both comparison costs and page access costs

The function performs sophisticated analysis of index clauses to determine which contribute to selectivity versus which only provide filtering, enabling more accurate cost estimates than generic approaches.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing global planning context and statistics
- `*path`: IndexPath structure describing the specific B-tree index access path being costed
- `loop_count`: Expected number of times this index scan will be executed (for nested loops)
- `*indexStartupCost`: Output parameter for one-time startup cost of the index scan
- `*indexTotalCost`: Output parameter for total cost including per-tuple processing
- `*indexSelectivity`: Output parameter for estimated fraction of table rows that will be returned
- `*indexCorrelation`: Output parameter for correlation between index and table ordering
- `*indexPages`: Output parameter for estimated number of index pages to be accessed
## Dependencies
- Functions called/Symbols referenced:
  - [genericcostestimate](../g/genericcostestimate.md)
  - [add_predicate_to_index_quals](../a/add_predicate_to_index_quals.md)
  - [get_op_opfamily_strategy](../g/get_op_opfamily_strategy.md)
  - [estimate_array_length](../e/estimate_array_length.md)
  - [clauselist_selectivity](../c/clauselist_selectivity.md)
  - planner_rt_fetch
  - [get_attstatsslot](../g/get_attstatsslot.md)
  - [get_opfamily_member](../g/get_opfamily_member.md)
  - [SearchSysCache3](../S/SearchSysCache3.md)
  - ReleaseVariableStats
- Called from (representative examples):
  - [bthandler](bthandler.md) (B-tree access method handler)

## Notes and Other Information
- Implements intelligent clamping of ScalarArrayOpExpr scan estimates to at most 1/3 of total index pages
- Charges logarithmic CPU cost for B-tree descent (log2(N) comparisons for N leaf tuples)
- Adds fixed CPU cost per page traversed during descent to account for bloated indexes
- Uses statistics correlation with adjustment factor (0.75) for multi-column indexes
- Handles both simple variables and expression indexes for correlation calculation
- Optimizes for the common case of unique indexes with complete equality conditions
- Distinguishes between boundary qualifiers (affecting selectivity) and filter qualifiers (affecting heap access only)

## Simplified Source

```c
void btcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
                   Cost *indexStartupCost, Cost *indexTotalCost,
                   Selectivity *indexSelectivity, double *indexCorrelation,
                   double *indexPages)
{
    IndexOptInfo *index = path->indexinfo;
    GenericCosts costs = {0};
    double numIndexTuples;
    Cost descentCost;
    List *indexBoundQuals = NIL;
    bool found_complete_equality = false;
    double num_sa_scans = 1;

    // Analyze boundary qualifiers - only leading = quals plus next inequality count
    int indexcol = 0;
    bool eqQualHere = false;
    bool found_saop = false;
    bool found_is_null_op = false;

    // Process index clauses to identify boundary quals
    ListCell *lc;
    foreach(lc, path->indexclauses) {
        IndexClause *iclause = lfirst_node(IndexClause, lc);

        // Check column progression for boundary analysis
        if (indexcol != iclause->indexcol) {
            if (!eqQualHere) break;  // Stop at first non-equality
            eqQualHere = false;
            indexcol++;
        }

        // Examine each qual in this clause
        ListCell *lc2;
        foreach(lc2, iclause->indexquals) {
            RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc2);
            Expr *clause = rinfo->clause;

            // Handle different clause types
            if (IsA(clause, OpExpr)) {
                OpExpr *op = (OpExpr *) clause;
                int strategy = get_op_opfamily_strategy(op->opno, index->opfamily[indexcol]);
                if (strategy == BTEqualStrategyNumber)
                    eqQualHere = true;
            }
            else if (IsA(clause, ScalarArrayOpExpr)) {
                ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) clause;
                double alength = estimate_array_length(root, lsecond(saop->args));
                found_saop = true;
                if (alength > 1)
                    num_sa_scans *= alength;
            }
            else if (IsA(clause, NullTest)) {
                NullTest *nt = (NullTest *) clause;
                if (nt->nulltesttype == IS_NULL) {
                    found_is_null_op = true;
                    eqQualHere = true;
                }
            }

            indexBoundQuals = lappend(indexBoundQuals, rinfo);
        }
    }

    // Unique index optimization for complete equality
    if (index->unique &&
        indexcol == index->nkeycolumns - 1 &&
        eqQualHere && !found_saop && !found_is_null_op) {
        numIndexTuples = 1.0;
        found_complete_equality = true;
    }
    else {
        // Calculate selectivity for boundary conditions
        List *selectivityQuals = add_predicate_to_index_quals(index, indexBoundQuals);
        Selectivity btreeSelectivity = clauselist_selectivity(root, selectivityQuals,
                                                             index->rel->relid, JOIN_INNER, NULL);
        numIndexTuples = btreeSelectivity * index->rel->tuples;

        // Clamp ScalarArrayOp scans to reasonable bounds
        num_sa_scans = Min(num_sa_scans, ceil(index->pages * 0.3333333));
        num_sa_scans = Max(num_sa_scans, 1);

        numIndexTuples = rint(numIndexTuples / num_sa_scans);
    }

    // Use generic cost estimation as base
    costs.numIndexTuples = numIndexTuples;
    costs.num_sa_scans = num_sa_scans;
    genericcostestimate(root, path, loop_count, &costs);

    // Add B-tree specific descent costs
    if (index->tuples > 1) {
        // CPU cost for tree descent: log2(N) comparisons
        descentCost = ceil(log(index->tuples) / log(2.0)) * cpu_operator_cost;
        costs.indexStartupCost += descentCost;
        costs.indexTotalCost += costs.num_sa_scans * descentCost;
    }

    // Additional CPU cost for page traversal
    descentCost = (index->tree_height + 1) * DEFAULT_PAGE_CPU_MULTIPLIER * cpu_operator_cost;
    costs.indexStartupCost += descentCost;
    costs.indexTotalCost += costs.num_sa_scans * descentCost;

    // Calculate index correlation from first column statistics
    VariableStatData vardata = {0};
    if (index->indexkeys[0] != 0) {
        // Get stats for underlying table column
        RangeTblEntry *rte = planner_rt_fetch(index->rel->relid, root);
        vardata.statsTuple = SearchSysCache3(STATRELATTINH,
                                           ObjectIdGetDatum(rte->relid),
                                           Int16GetDatum(index->indexkeys[0]),
                                           BoolGetDatum(rte->inh));
        vardata.freefunc = ReleaseSysCache;
    }
    else {
        // Get stats for index expression
        vardata.statsTuple = SearchSysCache3(STATRELATTINH,
                                           ObjectIdGetDatum(index->indexoid),
                                           Int16GetDatum(1),
                                           BoolGetDatum(false));
        vardata.freefunc = ReleaseSysCache;
    }

    // Extract correlation if statistics available
    if (HeapTupleIsValid(vardata.statsTuple)) {
        AttStatsSlot sslot;
        Oid sortop = get_opfamily_member(index->opfamily[0], index->opcintype[0],
                                       index->opcintype[0], BTLessStrategyNumber);
        if (OidIsValid(sortop) &&
            get_attstatsslot(&sslot, vardata.statsTuple, STATISTIC_KIND_CORRELATION,
                           sortop, ATTSTATSSLOT_NUMBERS)) {
            double varCorrelation = sslot.numbers[0];
            if (index->reverse_sort[0])
                varCorrelation = -varCorrelation;

            // Adjust for multi-column indexes
            costs.indexCorrelation = (index->nkeycolumns > 1) ?
                                   varCorrelation * 0.75 : varCorrelation;
            free_attstatsslot(&sslot);
        }
    }

    ReleaseVariableStats(vardata);

    // Return results
    *indexStartupCost = costs.indexStartupCost;
    *indexTotalCost = costs.indexTotalCost;
    *indexSelectivity = costs.indexSelectivity;
    *indexCorrelation = costs.indexCorrelation;
    *indexPages = costs.numIndexPages;
}
```