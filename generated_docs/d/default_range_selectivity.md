# default_range_selectivity

## Location
[src/backend/utils/adt/rangetypes_selfuncs.c:67-107](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_selfuncs.c#L67-L107)

## Overview
Provides default selectivity estimates for range operators when statistics are unavailable or cannot be used.

## Definition

```c
static double
default_range_selectivity(Oid operator)
```
## Detailed Description
This function returns hardcoded selectivity estimates for various range operators when PostgreSQL's query planner cannot rely on table statistics. The function uses a switch statement to map different range operator OIDs to appropriate selectivity values. These estimates are crucial for query optimization when dealing with range types in the absence of statistical information.

The selectivity values are carefully chosen based on the expected frequency and selectivity characteristics of each operator type:
- Overlap operations are considered moderately selective (0.01)  
- Contains/contained operations are highly selective (0.005)
- Element containment operations use the same selectivity as scalar range inequalities
- Comparison and positioning operators use standard inequality selectivity

## Parameters / Member Variables
- `operator`: The OID of the range operator for which to estimate selectivity
## Dependencies
- Functions called/Symbols referenced:
  - DEFAULT_RANGE_INEQ_SEL (constant for range inequality selectivity)
  - DEFAULT_INEQ_SEL (constant for standard inequality selectivity)
- Called from (representative examples):
  - [rangesel](../r/rangesel.md)
  - [calc_rangesel](../c/calc_rangesel.md)

## Notes and Other Information
- This function is static and only used within the rangetypes_selfuncs.c file
- The selectivity estimates are conservative fallbacks when no better information is available
- Different operator types receive different selectivity estimates based on their expected behavior:
  - Overlap operators: 0.01 (1%)
  - Contains/contained operators: 0.005 (0.5%) 
  - Element containment uses DEFAULT_RANGE_INEQ_SEL
  - Comparison operators use DEFAULT_INEQ_SEL
  - Unknown operators default to 0.01 as a safety measure

## Simplified Source

```c
static double default_range_selectivity(Oid operator) {
    switch (operator) {
        // Overlap operations - moderately selective
        case OID_RANGE_OVERLAP_OP:
            return 0.01;

        // Contains/contained operations - highly selective
        case OID_RANGE_CONTAINS_OP:
        case OID_RANGE_CONTAINED_OP:
            return 0.005;

        // Element containment - similar to scalar range inequality
        case OID_RANGE_CONTAINS_ELEM_OP:
        case OID_RANGE_ELEM_CONTAINED_OP:
            return DEFAULT_RANGE_INEQ_SEL;

        // Comparison and positioning operators - standard inequality selectivity
        case OID_RANGE_LESS_OP:
        case OID_RANGE_LESS_EQUAL_OP:
        case OID_RANGE_GREATER_OP:
        case OID_RANGE_GREATER_EQUAL_OP:
        case OID_RANGE_LEFT_OP:
        case OID_RANGE_RIGHT_OP:
        case OID_RANGE_OVERLAPS_LEFT_OP:
        case OID_RANGE_OVERLAPS_RIGHT_OP:
            return DEFAULT_INEQ_SEL;

        // Safety fallback for unknown operators
        default:
            return 0.01;
    }
}
```