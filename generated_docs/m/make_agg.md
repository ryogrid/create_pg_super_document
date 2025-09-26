# make_agg

## Location
[src/backend/optimizer/plan/createplan.c:6594-6627](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L6594-L6627)

## Overview
Creates and initializes an Agg (Aggregate) plan node for executing aggregation operations in PostgreSQL query plans.

## Definition
```c
Agg *make_agg(List *tlist, List *qual,
              AggStrategy aggstrategy, AggSplit aggsplit,
              int numGroupCols, AttrNumber *grpColIdx, Oid *grpOperators, Oid *grpCollations,
              List *groupingSets, List *chain, double dNumGroups,
              Size transitionSpace, Plan *lefttree)
```

## Detailed Description
This function constructs an Agg plan node that represents aggregation operations in PostgreSQLs query execution tree. The Agg node handles various aggregation strategies including hashing, sorting, and grouping sets. It initializes all the necessary fields for the aggregation operation including grouping columns, operators, collations, and estimated cardinalities. The function uses `clamp_cardinality_to_long` to safely convert the double precision group count estimate to a long integer while preventing overflow.

## Parameters / Member Variables
- `tlist`: Target list defining the output columns of the aggregation
- `qual`: Qualification conditions (WHERE/HAVING clauses) to be applied
- `aggstrategy`: Strategy for performing aggregation (hashing, sorting, etc.)
- `aggsplit`: How aggregation is split across multiple phases for parallel processing
- `numGroupCols`: Number of grouping columns
- `grpColIdx`: Array of attribute numbers for grouping columns
- `grpOperators`: Array of operator OIDs for grouping column comparisons
- `grpCollations`: Array of collation OIDs for grouping columns
- `groupingSets`: List of grouping sets for advanced GROUP BY operations
- `chain`: Chain of related aggregation nodes for multi-phase aggregation
- `dNumGroups`: Estimated number of groups as a double precision value
- `transitionSpace`: Memory space estimate for aggregate transition values
- `lefttree`: Left child plan node providing input tuples

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create Agg node)
  - [clamp_cardinality_to_long](../c/clamp_cardinality_to_long.md) (to safely convert group count estimate)
- Types referenced:
  - AggStrategy (enumeration for aggregation strategies)
  - AggSplit (enumeration for aggregation splitting)
  - [Agg](../A/Agg.md) (the aggregation plan node structure)
- Called from (representative examples):
  - [create_agg_plan](../c/create_agg_plan.md)
  - [create_unique_plan](../c/create_unique_plan.md)
  - [create_groupingsets_plan](../c/create_groupingsets_plan.md)

## Notes and Other Information
- The function sets `aggParams` to NULL as it will be filled later by `SS_finalize_plan()`
- Uses `clamp_cardinality_to_long` to handle potential overflow when converting double to long for group count estimates
- The right child plan node is always set to NULL as aggregation is a unary operation
- Part of PostgreSQLs cost-based query optimizer infrastructure for creating execution plans