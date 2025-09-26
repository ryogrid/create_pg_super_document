# QualCost

## Location
[src/include/nodes/pathnodes.h:45-49](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L45-L49)

## Overview
QualCost is a structure that represents cost estimates for query qualifiers (WHERE clauses, join conditions, etc.), containing both one-time startup costs and per-tuple evaluation costs.

## Definition

```c
typedef struct QualCost
{
	Cost		startup;		/* one-time cost */
	Cost		per_tuple;		/* per-evaluation cost */
} QualCost;
```
## Detailed Description
QualCost is a fundamental cost estimation structure used throughout PostgreSQL's query planner to track the computational cost of evaluating qualifiers (conditions). The structure separates costs into two categories: startup costs that are incurred once regardless of the number of tuples processed, and per-tuple costs that are multiplied by the expected number of tuples. This separation allows the planner to make more accurate cost comparisons between different execution strategies, especially when dealing with operations that have different startup vs. per-tuple cost profiles.

## Parameters / Member Variables
- : One-time initialization cost incurred when beginning to evaluate the qualifier, regardless of tuple count
- : Cost incurred for each tuple evaluation of the qualifier

## Dependencies
- Functions called/Symbols referenced:
  - Cost (typedef for cost values)
- Called from (representative examples):
  - [cost_qual_eval](../c/cost_qual_eval.md) (primary function for computing qualifier costs)
  - [cost_seqscan](../c/cost_seqscan.md) (sequential scan costing)
  - [cost_index](../c/cost_index.md) (index scan costing)
  - [final_cost_nestloop](../f/final_cost_nestloop.md) (nested loop join costing)
  - [final_cost_mergejoin](../f/final_cost_mergejoin.md) (merge join costing)
  - [final_cost_hashjoin](../f/final_cost_hashjoin.md) (hash join costing)

## Notes and Other Information
- Used extensively in PostgreSQL's cost-based optimizer for estimating the expense of evaluating WHERE clauses, join conditions, and other boolean expressions
- The separation of startup and per-tuple costs enables accurate modeling of operations with different cost characteristics
- Essential for path comparison in query planning, helping the optimizer choose the most efficient execution strategy
- Populated by cost_qual_eval() and related functions in costsize.c