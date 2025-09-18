# getWeights

## Location
[src/backend/utils/adt/tsrank.c:400-437](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsrank.c#L400-L437)

## Overview
Validates and processes weight arrays for text search ranking, returning either user-provided weights or default system weights with proper error handling.

## Definition
```c
static const float *getWeights(ArrayType *win)
```

## Detailed Description
This function serves as a weight validation and preparation utility for PostgreSQL's text search ranking system. It processes user-provided weight arrays or returns default weights when none are specified. The function performs comprehensive validation to ensure the weight array meets all requirements:

1. Returns default weights if no array is provided (win == NULL)
2. Validates that the array is one-dimensional
3. Ensures the array has sufficient length (at least lengthof(weights) elements)
4. Checks that no null values exist in the array
5. Validates that all weights are within the valid range (0.0 to 1.0)
6. Allows negative values to be replaced with corresponding default weights

The function uses a static array to store processed weights, making it efficient for repeated calls while ensuring the returned pointer remains valid throughout the ranking computation.

## Parameters / Member Variables
- `win`: ArrayType pointer containing user-provided weights, or NULL to use defaults

## Dependencies
- Functions called/Symbols referenced:
  - ARR_NDIM
  - ARR_DIMS
  - ArrayGetNItems
  - [array_contains_nulls](../a/array_contains_nulls.md)
  - ARR_DATA_PTR
  - lengthof
  - ereport/errcode/errmsg (error reporting functions)
- Called from (representative examples):
  - [ts_rank_wttf](../t/ts_rank_wttf.md)
  - [ts_rank_wtt](../t/ts_rank_wtt.md)
  - [ts_rank_ttf](../t/ts_rank_ttf.md)
  - [ts_rank_tt](../t/ts_rank_tt.md)
  - [ts_rankcd_wttf](../t/ts_rankcd_wttf.md)
  - [ts_rankcd_wtt](../t/ts_rankcd_wtt.md)
  - [ts_rankcd_ttf](../t/ts_rankcd_ttf.md)
  - [ts_rankcd_tt](../t/ts_rankcd_tt.md)

## Notes and Other Information
- Uses static storage for processed weights to maintain pointer validity
- Allows negative input values to be replaced with default weights for flexibility
- Enforces strict validation with descriptive error messages for debugging
- Weight values must be in range [0.0, 1.0] with higher values indicating greater importance
- Default weights are referenced from a global `weights` array when user weights are not provided
- The function is used by both regular ts_rank and ts_rankcd (cover density) functions