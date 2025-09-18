# reduce_outer_joins

## Location
src/backend/optimizer/prep/prepjointree.c: 2933 - 3005

## Overview
Attempts to reduce outer joins to plain inner joins by analyzing strictness constraints, and converts other outer join patterns into more efficient forms like anti-semijoins.

## Definition
```c
void reduce_outer_joins(PlannerInfo *root)
```

## Detailed Description
This function performs several important optimizations on outer joins in the query tree:

1. **Outer-to-Inner Join Reduction**: Converts LEFT/RIGHT/FULL JOINs to INNER JOINs when strict WHERE clauses make null-extended rows impossible. For example, `SELECT ... FROM a LEFT JOIN b ON (...) WHERE b.y = 42` can become an inner join since the strict equality operator will fail on null values from the outer join.

2. **Anti-Semijoin Recognition**: Identifies patterns like `SELECT ... FROM a LEFT JOIN b ON (a.x = b.y) WHERE b.y IS NULL` and converts them to anti-semijoins (JOIN_ANTI), which are more efficient to execute.

3. **Join Direction Normalization**: Converts RIGHT JOINs to LEFT JOINs to reduce the number of join types that need to be handled by later planner phases.

The function operates in two passes:
- **Pass 1**: Gathers information about base relations and outer joins below each join node
- **Pass 2**: Examines qual clauses and modifies join types based on strictness analysis

After join reduction, the function removes nulling relation references that are no longer needed, handling both fully-reduced joins and partially-reduced full joins.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing the query parse tree and planner state

## Dependencies
- Functions called/Symbols referenced:
  - reduce_outer_joins_pass1
  - reduce_outer_joins_pass2
  - bms_is_empty
  - remove_nulling_relids
  - bms_make_singleton
  - elog
- Called from (representative examples):
  - subquery_planner

## Notes and Other Information
- Must be run after expression preprocessing (qual canonicalization and JOIN alias-var expansion)
- The optimization can significantly improve query performance by reducing join complexity
- Handles both complete join reduction (outer to inner) and partial reduction (FULL joins)
- Uses strictness analysis to determine when null-extended rows can never satisfy query conditions
- The function expects that outer joins exist in the query tree (enforced by error check)
- Declared in src/include/optimizer/prep.h as part of the query preparation phase