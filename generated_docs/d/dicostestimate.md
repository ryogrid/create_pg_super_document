# dicostestimate

## Location
[src/test/modules/dummy_index_am/dummy_index_am.c:204-223](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/dummy_index_am/dummy_index_am.c#L204-L223)

## Overview
A cost estimation function for the dummy index access method that intentionally returns prohibitively high costs to prevent the PostgreSQL planner from using this index in query plans.

## Definition

```c
static void
dicostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
			   Cost *indexStartupCost, Cost *indexTotalCost,
			   Selectivity *indexSelectivity, double *indexCorrelation,
			   double *indexPages)
```
## Detailed Description
The  function implements the cost estimation interface for the dummy index access method. Unlike production index AMs that provide realistic cost estimates to help the planner choose efficient query plans, this function deliberately returns extremely high costs (1.0e10) for both startup and total costs.

This design ensures that the PostgreSQL query planner will never select the dummy index for actual query execution, which is appropriate since the dummy AM doesn't provide real index functionality. The other output parameters are set to minimal placeholder values since they won't influence planning decisions given the prohibitive costs.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing query planning context and statistics
- `*path`: IndexPath structure representing the potential index scan path being costed
- `loop_count`: Expected number of times this index scan would be executed
- `*indexStartupCost`: Output parameter for one-time startup cost (set to 1.0e10)
- `*indexTotalCost`: Output parameter for total execution cost (set to 1.0e10)
- `*indexSelectivity`: Output parameter for estimated selectivity (set to 1)
- `*indexCorrelation`: Output parameter for correlation with table ordering (set to 0)
- `*indexPages`: Output parameter for estimated index size in pages (set to 1)
## Dependencies
- Functions called/Symbols referenced:
  - [PlannerInfo](../P/PlannerInfo.md) (structure type)
  - [IndexPath](../I/IndexPath.md) (structure type)
  - Cost (type alias)
  - Selectivity (type alias)
- Called from (representative examples):
  - [dihandler](dihandler.md) (dummy index AM handler registration)

## Notes and Other Information
- This is a test module function designed to prevent actual usage in query planning
- Sets prohibitively high costs (1.0e10) to ensure the planner never selects this index
- Part of the dummy_index_am test module framework
- Other parameters are set to placeholder values since high costs make them irrelevant
- Follows the standard PostgreSQL index AM cost estimation interface specification

## Simplified Source

```c
static void
dicostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
               Cost *indexStartupCost, Cost *indexTotalCost,
               Selectivity *indexSelectivity, double *indexCorrelation,
               double *indexPages)
{
    // Tell planner to never use this index by setting prohibitive costs
    *indexStartupCost = 1.0e10;
    *indexTotalCost = 1.0e10;

    // Set placeholder values for other parameters
    *indexSelectivity = 1;
    *indexCorrelation = 0;
    *indexPages = 1;
}
```