# AggPath

## Location
[src/include/nodes/pathnodes.h:2253-2263](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L2253-L2263)

## Overview
AggPath represents a query execution path node that performs aggregate function computation and grouping operations, supporting both sorted and hashed grouping strategies.

## Definition
```c
typedef struct AggPath
{
    Path        path;
    Path       *subpath;        /* path representing input source */
    AggStrategy aggstrategy;    /* basic strategy, see nodes.h */
    AggSplit    aggsplit;       /* agg-splitting mode, see nodes.h */
    Cardinality numGroups;      /* estimated number of groups in input */
    uint64      transitionSpace; /* for pass-by-ref transition data */
    List       *groupClause;    /* a list of SortGroupClause's */
    List       *qual;           /* quals (HAVING quals), if any */
} AggPath;
```

## Detailed Description
AggPath is a comprehensive path node in PostgreSQL's query planner that represents aggregation and grouping operations. It supports various aggregation strategies including sorted grouping (AGG_SORTED) where input must be presorted, and hashed grouping (AGG_HASHED) which can work with unsorted input.

The path handles both simple aggregation (without GROUP BY) and complex grouping scenarios. It incorporates cost estimation for aggregate function computation, memory usage for transition data, and supports aggregate splitting for parallel execution. The path can also apply post-aggregation filtering through HAVING clauses.

AggPath is fundamental to implementing SQL aggregate queries including SUM, COUNT, AVG, and other aggregate functions, with or without GROUP BY clauses.

## Parameters / Member Variables
- `path`: Base Path structure containing common path information (cost, parent relation, target, pathkeys, etc.)
- `subpath`: Pointer to the input Path node providing source data for aggregation
- `aggstrategy`: Enumerated strategy for aggregation (AGG_PLAIN, AGG_SORTED, AGG_HASHED, etc.)
- `aggsplit`: Aggregate splitting mode for parallel processing (AGGSPLIT_SIMPLE, AGGSPLIT_INITIAL_SERIAL, etc.)
- `numGroups`: Estimated number of groups in the result (1 for non-grouping aggregation)
- `transitionSpace`: Estimated memory space needed for pass-by-reference aggregate transition data
- `groupClause`: List of SortGroupClause structures defining the grouping columns
- `qual`: List of qualification expressions representing HAVING clauses applied after aggregation

## Dependencies
- Functions called/Symbols referenced:
  - [Path](../P/Path.md) (base structure)
  - AggStrategy (aggregation strategy enumeration)
  - AggSplit (aggregate splitting mode enumeration)
  - [List](../L/List.md) (for groupClause and qual)
  - [SortGroupClause](../S/SortGroupClause.md) (grouping specifications)
  - Cardinality (row count estimation type)
- Called from (representative examples):
  - [create_agg_path](../c/create_agg_path.md) (creates AggPath instances)
  - [create_agg_plan](../c/create_agg_plan.md) (converts AggPath to execution plan)
  - [create_plan_recurse](../c/create_plan_recurse.md) (part of plan creation process)

## Notes and Other Information
- For AGG_SORTED strategy, input must be presorted on grouping columns
- AGG_HASHED strategy can work with unsorted input but requires memory for hash tables
- [Path](../P/Path.md) ordering is preserved for AGG_SORTED, but output is unordered for AGG_HASHED
- Cost estimation includes aggregate function computation costs and memory usage
- Supports parallel aggregate execution through aggsplit modes
- transitionSpace is crucial for memory planning in complex aggregate operations
- HAVING clauses are applied after grouping and aggregation are complete