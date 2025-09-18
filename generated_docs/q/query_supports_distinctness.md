# query_supports_distinctness

## Location
src/backend/optimizer/plan/analyzejoins.c: 958 - 994

## Overview
A pre-checking function that determines whether a query could possibly be proven distinct on some set of output columns, serving as an optimization to avoid expensive analysis when distinctness is impossible.

## Definition
```c
bool query_supports_distinctness(Query *query)
```

## Detailed Description
This function performs a lightweight analysis to determine if the given query structure has features that could potentially make it provably distinct. It serves as an optimization barrier before more expensive distinctness analysis - if this function returns false, there's no point in calling `query_is_distinct_for()` as it would definitely return false.

The function checks for PostgreSQL query features that can guarantee or enable distinctness:
- DISTINCT clauses explicitly ensure distinctness
- GROUP BY operations naturally produce distinct groupings
- Grouping sets create distinct result groups
- Aggregate functions with grouping produce distinct results per group
- HAVING clauses filter grouped results maintaining distinctness
- Set operations (UNION, INTERSECT, EXCEPT) can produce distinct results

A special case handles set-returning functions (SRFs): they break distinctness unless an explicit DISTINCT clause is present, since SRFs can generate multiple rows from a single input row.

## Parameters / Member Variables
- `query`: Pointer to the Query node to analyze for potential distinctness

## Dependencies
- Functions called/Symbols referenced:
  - NIL (PostgreSQL null list constant)
- Called from:
  - [rel_supports_distinctness](../r/rel_supports_distinctness.md) (src/backend/optimizer/plan/analyzejoins.c:835)
  - [create_unique_path](../c/create_unique_path.md) (src/backend/optimizer/util/pathnode.c:1754)

## Notes and Other Information
- This function is designed for performance - it should not perform expensive computations
- Returns true for potential distinctness, false only when distinctness is definitely impossible  
- The presence of target SRFs without DISTINCT is specifically handled as it breaks distinctness guarantees
- Part of PostgreSQL's query optimization framework for join elimination and path generation