# get_restriction_qual_cost

## Location
[src/backend/optimizer/path/costsize.c:4965-5006](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L4965-L5006)

## Overview
Computes the combined evaluation costs of a base relation's restriction qualifiers and any movable join quals pushed down to the scan.

## Definition

```c
static void
get_restriction_qual_cost(PlannerInfo *root, RelOptInfo *baserel,
						  ParamPathInfo *param_info,
						  QualCost *qpqual_cost)
```
## Detailed Description
The  function provides a convenient way to calculate the total cost of evaluating all qualification clauses that will be applied during a base relation scan. It handles two scenarios:

**With Parameter Path Info**: When  is provided, the function includes costs for additional join clauses that have been pushed down to the scan level for parameterized paths. It calls  to compute costs for the pushed-down clauses and adds them to the base relation's pre-computed restriction costs.

**Without Parameter Path Info**: When  is NULL, it simply returns the base relation's pre-computed restriction costs (baserestrictcost), which were previously calculated by .

This function is designed for scan types that evaluate all qualifications "the hard way" during tuple retrieval, making it suitable for sequential scans and similar operations but not for index scans where the index machinery handles some qualifications.

## Parameters / Member Variables
- : PlannerInfo context containing global planning information
- : RelOptInfo structure for the base relation being scanned
- : ParamPathInfo containing pushed-down join clauses (can be NULL)
- : Output parameter receiving the total qualification evaluation costs

## Dependencies
- Functions called/Symbols referenced:
  - [cost_qual_eval](../c/cost_qual_eval.md)
  - [ParamPathInfo](../P/ParamPathInfo.md) (struct)
  - [QualCost](../Q/QualCost.md) (struct)
- Called from (representative examples):
  - [cost_seqscan](../c/cost_seqscan.md)
  - [cost_samplescan](../c/cost_samplescan.md)
  - [cost_bitmap_heap_scan](../c/cost_bitmap_heap_scan.md)
  - [cost_tidscan](../c/cost_tidscan.md)
  - [cost_tidrangescan](../c/cost_tidrangescan.md)
  - [cost_subqueryscan](../c/cost_subqueryscan.md)
  - [cost_functionscan](../c/cost_functionscan.md)
  - [cost_tablefuncscan](../c/cost_tablefuncscan.md)

## Notes and Other Information
- Static function within costsize.c, used internally by scan cost estimation routines
- Assumes baserestrictcost was previously computed by set_baserel_size_estimates()
- Not suitable for index scans where index machinery evaluates some qualifications
- Handles parameterized paths by including costs of pushed-down join clauses
- Provides a uniform interface for qualification cost calculation across multiple scan types
- Essential for accurate cost estimation in scan nodes that perform full qualification evaluation

## Simplified Source

```c
static void
get_restriction_qual_cost(PlannerInfo *root, RelOptInfo *baserel,
                          ParamPathInfo *param_info,
                          QualCost *qpqual_cost)
{
    if (param_info) {
        // Calculate costs for pushed-down join clauses
        cost_qual_eval(qpqual_cost, param_info->ppi_clauses, root);

        // Add base relation restriction costs
        qpqual_cost->startup += baserel->baserestrictcost.startup;
        qpqual_cost->per_tuple += baserel->baserestrictcost.per_tuple;
    } else {
        // Just use pre-computed base restriction costs
        *qpqual_cost = baserel->baserestrictcost;
    }
}
```