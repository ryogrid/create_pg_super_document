# cost_resultscan

## Location
src/backend/optimizer/path/costsize.c: 1776 - 1812

## Overview
Determines and returns the cost of scanning an RTE_RESULT relation, which represents result sets that don't correspond to actual tables but generate computed results.

## Definition


## Detailed Description
This function calculates the execution cost for scanning an RTE_RESULT relation. RTE_RESULT relations are special range table entries that represent result sets generated through computation rather than table scans, such as:

1. **Computed results**: Results from expressions or function calls that don't correspond to physical tables
2. **Virtual relations**: Relations that exist conceptually but don't have underlying storage
3. **Synthesized data**: Data generated on-the-fly during query execution

The cost model is simpler than physical table scans since there's no I/O involved, focusing primarily on:
- CPU costs for tuple generation and processing
- Qualification costs for applying WHERE clause restrictions
- Standard tuple processing overhead

## Parameters / Member Variables
- : The Path node to store the calculated costs (startup_cost and total_cost fields are set)
- : PlannerInfo structure containing global planning information and cost parameters
- : RelOptInfo representing the result relation (must have rtekind == RTE_RESULT)
- : ParamPathInfo for parameterized paths, or NULL for non-parameterized scans

## Dependencies
- Functions called/Symbols referenced:
  - get_restriction_qual_cost (calculates cost of applying restriction qualifiers)
  - cpu_tuple_cost (global cost parameter for tuple processing)
- Types referenced:
  - ParamPathInfo (parameterized path information)
  - Cost (cost calculation type)
  - QualCost (qualification cost structure)
  - RTE_RESULT (enum value for result range table entries)
- Called from:
  - create_resultscan_path (in pathnode.c:2191)

## Notes and Other Information
- The function includes an assertion ensuring the relation is a result relation (rtekind == RTE_RESULT)
- Uses a simpler cost model than physical scans, charging only cpu_tuple_cost plus qualification costs
- Does not include target list evaluation costs separately, suggesting they may be minimal or handled elsewhere for result relations
- No I/O costs are involved since result relations don't correspond to physical storage
- The cost calculation reflects that result relations typically involve computational work rather than data retrieval
- Designed for relations that generate results through computation rather than storage access