# default_multirange_selectivity

## Location
src/backend/utils/adt/multirangetypes_selfuncs.c: 78 - 136

## Overview
Returns a default selectivity estimate for multirange operators when statistics are unavailable or cannot be used.

## Definition


## Detailed Description
This function provides fallback selectivity estimates for various multirange operators when PostgreSQL cannot use statistics-based calculations. It maps different multirange operator OIDs to appropriate default probability values based on the expected behavior and frequency of matches for each operator type.

The function categorizes operators into several groups:
- Overlap operators (&&): Return 0.01 (1% selectivity)
- Containment operators (@>, <@): Return 0.005 (0.5% selectivity) 
- Element containment (@>): Uses DEFAULT_MULTIRANGE_INEQ_SEL constant
- Comparison and positional operators (<, <=, >, >=, <<, >>, &<, &>): Use DEFAULT_INEQ_SEL constant (similar to scalar inequalities)

## Parameters / Member Variables
- : The OID of the multirange operator for which to estimate selectivity

## Dependencies
- Functions called/Symbols referenced:
  - DEFAULT_MULTIRANGE_INEQ_SEL
  - DEFAULT_INEQ_SEL

- Called from (representative examples):
  - multirangesel (multiple times for fallback estimates)
  - calc_multirangesel (as fallback when histogram analysis fails)

## Notes and Other Information
The function serves as a safety net in the PostgreSQL query planner's selectivity estimation system. The specific probability values are based on empirical observations of typical data patterns and operator behavior. Overlap operations are considered relatively rare (1%), while containment operations are even rarer (0.5%). Element containment operations are treated similarly to scalar range comparisons since they represent point-in-range queries.