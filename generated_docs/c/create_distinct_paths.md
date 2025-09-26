# create_distinct_paths

## Location
[src/backend/optimizer/plan/planner.c:4830-4899](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L4830-L4899)

## Overview
Creates a new upper relation containing execution paths for SELECT DISTINCT evaluation, supporting both serial and parallel execution strategies while handling datatype compatibility requirements.

## Definition
```c
static RelOptInfo *create_distinct_paths(PlannerInfo *root, RelOptInfo *input_rel,
                                       PathTarget *target)
```

## Detailed Description
This function constructs the distinct relation (UPPERREL_DISTINCT) responsible for eliminating duplicate rows in SELECT DISTINCT queries. It creates execution paths using two complementary approaches to handle both regular and parallel query execution.

The function operates by:
- **Serial path creation**: Uses create_final_distinct_paths to generate standard Sort/Unique and Hash-based distinct elimination paths from the input relation's regular pathlist
- **Parallel path creation**: Uses create_partial_distinct_paths to generate paths that can execute distinct operations in parallel workers, processing partial results that can be combined later
- **Datatype validation**: Ensures that all involved datatypes support the required operations (either sorting or hashing) for distinct elimination
- **FDW integration**: Provides extension points for Foreign Data Wrappers to implement custom distinct operations

The function inherits parallel safety characteristics from the input relation, as distinct operations don't introduce additional parallel safety concerns beyond what's already present in the input data. For DISTINCT ON queries, the parallel safety depends on the expressions in the DISTINCT ON clause.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global query planning context and configuration settings
- `input_rel`: Source RelOptInfo containing input paths that provide data for distinct elimination
- `target`: PathTarget specifying the columns that should be present in the distinct result

## Dependencies
- Functions called/Symbols referenced:
  - [fetch_upper_rel](../f/fetch_upper_rel.md)
  - [create_final_distinct_paths](create_final_distinct_paths.md)  
  - [create_partial_distinct_paths](create_partial_distinct_paths.md)
  - [set_cheapest](../s/set_cheapest.md)
- Called from (representative examples):
  - [grouping_planner](../g/grouping_planner.md)

## Notes and Other Information
- Input paths are expected to already compute the desired target columns since Sort/Unique operations don't perform projection
- The function creates paths for the (UPPERREL_DISTINCT, NULL) upper relation level
- Both sorting-based (Sort + Unique) and hash-based distinct elimination strategies are considered
- Provides comprehensive error handling when no viable distinct implementation can be found, specifically highlighting datatype compatibility issues
- Supports Foreign Data Wrapper extensions through GetForeignUpperPaths callback for distributed distinct operations
- Extension hooks allow custom distinct path implementations through create_upper_paths_hook
- Critical for performance of queries with large result sets where duplicate elimination is expensive
- The choice between different distinct strategies depends on data characteristics, available memory, and sorting costs