# ts_rank_ttf

## Location
src/backend/utils/adt/tsrank.c: 471 - 485

## Overview
A PostgreSQL function that calculates the rank of a TSVector against a TSQuery using default weights and a custom normalization method.

## Definition


## Detailed Description
The  function is a PostgreSQL built-in function that computes text search ranking scores with custom normalization options. It takes three arguments: a TSVector representing indexed text, a TSQuery representing the search criteria, and an integer specifying the normalization method to apply. Unlike , this function uses default weights for the four word classes (D, C, B, A) by passing NULL to the getWeights function. The normalization method parameter allows control over how the final ranking score is adjusted based on document characteristics like length, uniqueness, etc.

## Parameters / Member Variables
-  (TSVector): The text search vector representing the indexed document content
-  (TSQuery): The text search query expression to match against the TSVector  
-  (int32): Normalization method flags that control how the rank score is normalized (e.g., RANK_NORM_LENGTH, RANK_NORM_UNIQ, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TSVECTOR: Extracts TSVector from function arguments
  - PG_GETARG_TSQUERY: Extracts TSQuery from function arguments  
  - PG_GETARG_INT32: Extracts the normalization method integer from function arguments
  - getWeights: Called with NULL to use default weight values
  - calc_rank: Performs the actual ranking calculation with the specified normalization method
  - PG_FREE_IF_COPY: Frees detoasted copies of arguments
  - PG_RETURN_FLOAT4: Returns the calculated rank as a float4 value
- Called from (representative examples):
  - SQL queries using ts_rank(tsvector, tsquery, normalization) function

## Notes and Other Information
- This is the ts_rank variant that accepts a custom normalization method parameter
- Uses default weights for all word classes since getWeights(NULL) returns the default weight array
- The normalization method parameter provides fine control over ranking score calculation, allowing combinations of different normalization techniques
- Commonly used normalization methods include length-based, uniqueness-based, and logarithmic variants
- The function automatically handles memory management by freeing detoasted argument copies
- Part of PostgreSQL's full-text search ranking system alongside other ts_rank variants