# bernoulli_samplescangetsamplesize

## Location
src/backend/access/tablesample/bernoulli.c: 86 - 126

## Overview
This function estimates the sample size (number of pages and tuples) that will be processed during a Bernoulli tablesample scan, used by PostgreSQL's query planner for cost estimation.

## Definition
```c
static void bernoulli_samplescangetsamplesize(PlannerInfo *root,
                                              RelOptInfo *baserel,
                                              List *paramexprs,
                                              BlockNumber *pages,
                                              double *tuples)
```

## Detailed Description
This function performs sample size estimation for the Bernoulli tablesample method during query planning. It extracts the sampling percentage from the parameter expressions, validates it, and calculates the expected number of pages and tuples that will be examined. Unlike block-level sampling methods, Bernoulli sampling visits all pages of the relation since it needs to examine every tuple to make individual sampling decisions. The function provides the planner with estimates needed for accurate cost calculations and plan selection.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planner context and statistics
- `baserel`: RelOptInfo structure with relation statistics (pages, tuples, etc.)
- `paramexprs`: List of parameter expressions, first element should be the sampling percentage
- `pages`: Output parameter for estimated number of pages to be scanned
- `tuples`: Output parameter for estimated number of tuples in the sample

## Dependencies
- Functions called/Symbols referenced:
  - [estimate_expression_value](../e/estimate_expression_value.md) (evaluates parameter expressions)
  - [DatumGetFloat4](../D/DatumGetFloat4.md) (extracts float4 value from Datum)
  - isnan (checks for NaN values)
  - [clamp_row_est](../c/clamp_row_est.md) (ensures row estimate is within valid bounds)
- Called from (representative examples):
  - [tsm_bernoulli_handler](../t/tsm_bernoulli_handler.md) (sets this as SampleScanGetSampleSize callback)

## Notes and Other Information
- Always visits all pages of the baserel since Bernoulli sampling examines every tuple
- Uses a default sample fraction of 0.1 (10%) if the parameter cannot be evaluated or is invalid
- Validates the sampling percentage to be between 0 and 100, and not NaN
- Converts percentage (0-100) to fraction (0.0-1.0) for internal calculations
- The estimated tuple count is clamped to ensure it stays within reasonable bounds for the planner
- This is a static function, only callable within the bernoulli.c module