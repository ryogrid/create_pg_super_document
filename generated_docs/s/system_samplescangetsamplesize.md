# system_samplescangetsamplesize

## Location
src/backend/access/tablesample/system.c: 88 - 129

## Overview
Estimates the sample size (number of pages and tuples) that will be examined during a SYSTEM table sampling operation based on the provided sampling percentage parameter.

## Definition
```c
static void system_samplescangetsamplesize(PlannerInfo *root,
                                         RelOptInfo *baserel,
                                         List *paramexprs,
                                         BlockNumber *pages,
                                         double *tuples)
```

## Detailed Description
This function is responsible for calculating how many pages and tuples the SYSTEM sampling method will examine during query planning. It extracts the sampling percentage from the parameter expressions, validates the value, and computes estimates for both the number of pages to visit and the expected number of tuples to retrieve. The function handles edge cases by providing sensible defaults when the sampling percentage cannot be determined or is invalid. These estimates are crucial for the query planner to make informed decisions about query execution strategies and cost calculations.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planning context and information
- `baserel`: RelOptInfo structure with relation statistics (pages, tuples, etc.)
- `paramexprs`: List of parameter expressions, expected to contain the sampling percentage
- `pages`: Output parameter for estimated number of pages to sample
- `tuples`: Output parameter for estimated number of tuples to retrieve

## Dependencies
- Functions called/Symbols referenced:
  - linitial (gets first element from parameter list)
  - [estimate_expression_value](../e/estimate_expression_value.md) (evaluates expression to get constant value)
  - IsA (type checking macro)
  - [DatumGetFloat4](../D/DatumGetFloat4.md) (extracts float4 value from Datum)
  - isnan (checks for NaN values)
  - [clamp_row_est](../c/clamp_row_est.md) (clamps row estimates to valid ranges)
- Called from (representative examples):
  - [tsm_system_handler](../t/tsm_system_handler.md) (as function pointer in TsmRoutine)
  - [Query](../Q/Query.md) planner during sampling cost estimation

## Notes and Other Information
- Uses a default sampling fraction of 0.1 (10%) when the parameter cannot be evaluated or is invalid
- Validates that the sampling percentage is between 0 and 100 and not NaN
- Converts percentage values to fractions (divides by 100)
- The estimates are used by PostgreSQL's query planner for cost calculations and optimization decisions
- Results are clamped using clamp_row_est to ensure they fall within valid ranges for the planner