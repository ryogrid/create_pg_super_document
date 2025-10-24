# default_multirange_selectivity

## Location
[src/backend/utils/adt/multirangetypes_selfuncs.c:78-136](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes_selfuncs.c#L78-L136)

## Overview
Returns a default selectivity estimate for multirange operators when statistics are unavailable or cannot be used.

## Definition

```c
static double
default_multirange_selectivity(Oid operator)
```
## Detailed Description
This function provides fallback selectivity estimates for various multirange operators when PostgreSQL cannot use statistics-based calculations. It maps different multirange operator OIDs to appropriate default probability values based on the expected behavior and frequency of matches for each operator type.

The function categorizes operators into several groups:
- Overlap operators (&&): Return 0.01 (1% selectivity)
- Containment operators (@>, <@): Return 0.005 (0.5% selectivity) 
- Element containment (@>): Uses DEFAULT_MULTIRANGE_INEQ_SEL constant
- Comparison and positional operators (<, <=, >, >=, <<, >>, &<, &>): Use DEFAULT_INEQ_SEL constant (similar to scalar inequalities)

## Parameters / Member Variables
- `operator`: The OID of the multirange operator for which to estimate selectivity
## Dependencies
- Functions called/Symbols referenced:
  - DEFAULT_MULTIRANGE_INEQ_SEL
  - DEFAULT_INEQ_SEL

- Called from (representative examples):
  - [multirangesel](../m/multirangesel.md) (multiple times for fallback estimates)
  - [calc_multirangesel](../c/calc_multirangesel.md) (as fallback when histogram analysis fails)

## Notes and Other Information
The function serves as a safety net in the PostgreSQL query planner's selectivity estimation system. The specific probability values are based on empirical observations of typical data patterns and operator behavior. Overlap operations are considered relatively rare (1%), while containment operations are even rarer (0.5%). Element containment operations are treated similarly to scalar range comparisons since they represent point-in-range queries.

## Simplified Source

```c
static double
default_multirange_selectivity(Oid operator)
{
    switch (operator)
    {
        // Overlap operators - moderately selective
        case OID_MULTIRANGE_OVERLAPS_MULTIRANGE_OP:
        case OID_MULTIRANGE_OVERLAPS_RANGE_OP:
        case OID_RANGE_OVERLAPS_MULTIRANGE_OP:
            return 0.01;

        // Containment operators - highly selective
        case OID_RANGE_CONTAINS_MULTIRANGE_OP:
        case OID_RANGE_MULTIRANGE_CONTAINED_OP:
        case OID_MULTIRANGE_CONTAINS_RANGE_OP:
        case OID_MULTIRANGE_CONTAINS_MULTIRANGE_OP:
        case OID_MULTIRANGE_RANGE_CONTAINED_OP:
        case OID_MULTIRANGE_MULTIRANGE_CONTAINED_OP:
            return 0.005;

        // Element containment - similar to scalar inequality
        case OID_MULTIRANGE_CONTAINS_ELEM_OP:
        case OID_MULTIRANGE_ELEM_CONTAINED_OP:
            return DEFAULT_MULTIRANGE_INEQ_SEL;

        // Positional and comparison operators
        case OID_MULTIRANGE_LESS_OP:
        case OID_MULTIRANGE_LESS_EQUAL_OP:
        case OID_MULTIRANGE_GREATER_OP:
        case OID_MULTIRANGE_GREATER_EQUAL_OP:
        case OID_MULTIRANGE_LEFT_RANGE_OP:
        case OID_MULTIRANGE_LEFT_MULTIRANGE_OP:
        case OID_RANGE_LEFT_MULTIRANGE_OP:
        case OID_MULTIRANGE_RIGHT_RANGE_OP:
        case OID_MULTIRANGE_RIGHT_MULTIRANGE_OP:
        case OID_RANGE_RIGHT_MULTIRANGE_OP:
        case OID_MULTIRANGE_OVERLAPS_LEFT_RANGE_OP:
        case OID_RANGE_OVERLAPS_LEFT_MULTIRANGE_OP:
        case OID_MULTIRANGE_OVERLAPS_LEFT_MULTIRANGE_OP:
        case OID_MULTIRANGE_OVERLAPS_RIGHT_RANGE_OP:
        case OID_RANGE_OVERLAPS_RIGHT_MULTIRANGE_OP:
        case OID_MULTIRANGE_OVERLAPS_RIGHT_MULTIRANGE_OP:
            return DEFAULT_INEQ_SEL;

        // Fallback for unknown operators
        default:
            return 0.01;
    }
}
```