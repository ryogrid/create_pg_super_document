# pushdown_safety_info

## Location
src/backend/optimizer/path/allpaths.c: 60 - 67

## Overview
A structure that holds safety information for determining whether qualifiers can be pushed down into a subquery during query optimization.

## Definition


## Detailed Description
The  structure is used by PostgreSQL's query optimizer to track safety constraints when considering pushing WHERE clause qualifiers down into subqueries. This optimization technique can significantly improve query performance by reducing the number of rows that need to be processed at higher levels of the query plan.

The structure serves as a container for various safety flags that indicate whether certain types of qualifiers can be safely pushed down without changing the query semantics or results. It's primarily used in conjunction with the  function and related qual pushdown analysis functions.

The safety information is collected during the analysis phase and later consulted by  to make final decisions about individual qualifiers.

## Parameters / Member Variables
- : A bitmask array where each element corresponds to a column in the subquery's target list. Each bitmask indicates specific reasons why that column is unsafe for qual pushdown (or 0 if the column is safe)
- : A boolean flag indicating that volatile qualifiers should not be pushed down into this subquery due to semantic constraints (e.g., DISTINCT, window functions, or set-returning functions)
- : A boolean flag indicating that leaky qualifiers should not be pushed down into this subquery for security reasons

## Dependencies
- Functions called/Symbols referenced:
  - (This is a data structure with no direct function calls)
- Called from (representative examples):
  -  (src/backend/optimizer/path/allpaths.c:3583)
  -  (src/backend/optimizer/path/allpaths.c:3639)
  -  (src/backend/optimizer/path/allpaths.c:3707)
  -  (src/backend/optimizer/path/allpaths.c:3780)
  -  (src/backend/optimizer/path/allpaths.c:3856)
  -  (src/backend/optimizer/path/allpaths.c:2489)

## Notes and Other Information
- This structure is specifically designed for qual pushdown optimization in the PostgreSQL query planner
- The safety analysis considers multiple factors including LIMIT clauses, DISTINCT operations, window functions, set-returning functions, and grouping sets
- The  array is particularly important for set operations where different arms of a UNION/INTERSECT/EXCEPT may have different safety characteristics for individual columns
- The volatile and leaky flags provide broad safety constraints that apply to entire classes of qualifiers rather than specific columns
- Proper use of this structure helps ensure that query optimizations maintain correct semantics while maximizing performance gains