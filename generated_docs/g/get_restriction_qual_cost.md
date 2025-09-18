# get_restriction_qual_cost

## Location
src/backend/optimizer/path/costsize.c: 4965 - 5006

## Overview
Computes the combined evaluation costs of a base relation's restriction qualifiers and any movable join quals pushed down to the scan.

## Definition


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
  - cost_qual_eval
  - ParamPathInfo (struct)
  - QualCost (struct)
- Called from (representative examples):
  - cost_seqscan
  - cost_samplescan
  - cost_bitmap_heap_scan
  - cost_tidscan
  - cost_tidrangescan
  - cost_subqueryscan
  - cost_functionscan
  - cost_tablefuncscan

## Notes and Other Information
- Static function within costsize.c, used internally by scan cost estimation routines
- Assumes baserestrictcost was previously computed by set_baserel_size_estimates()
- Not suitable for index scans where index machinery evaluates some qualifications
- Handles parameterized paths by including costs of pushed-down join clauses
- Provides a uniform interface for qualification cost calculation across multiple scan types
- Essential for accurate cost estimation in scan nodes that perform full qualification evaluation