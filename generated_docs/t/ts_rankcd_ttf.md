# ts_rankcd_ttf

## Location
src/backend/utils/adt/tsrank.c: 986 - 1000

## Overview
PostgreSQL function that computes the cover density ranking for text search vectors using default weights, text vector, query, and custom normalization method.

## Definition
```c
Datum ts_rankcd_ttf(PG_FUNCTION_ARGS)
```

## Detailed Description
`ts_rankcd_ttf` is a PostgreSQL built-in function that calculates the cover density ranking for text search operations. It takes three arguments: a tsvector (text search vector), a tsquery (text search query), and a normalization method flag, using default weights for lexeme categories. This function allows customization of the normalization method while using standard lexeme weights.

The function uses NULL for the weights parameter, which causes `getWeights` to return default weight values, providing standard weighting behavior for different lexeme categories (A, B, C, D).

## Parameters / Member Variables
- `txt` (TSVector): Text search vector containing lexemes and their positions
- `query` (TSQuery): Text search query specifying the terms to match  
- `method` (int32): Normalization method flag controlling how the ranking score is normalized

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TSVECTOR: Extracts tsvector argument from function call
  - PG_GETARG_TSQUERY: Extracts tsquery argument from function call
  - PG_GETARG_INT32: Extracts int32 argument from function call
  - [getWeights](../g/getWeights.md): Processes NULL to return default weight values
  - [calc_rank_cd](../c/calc_rank_cd.md): Core cover density ranking calculation function
  - PG_FREE_IF_COPY: Frees memory for copied arguments if needed
  - PG_RETURN_FLOAT4: Returns float4 result to PostgreSQL
- Called from (representative examples):
  - None directly (SQL function interface)

## Notes and Other Information
This function is part of the ts_rankcd family of functions, specifically designed for cases where users want to experiment with different normalization methods but are satisfied with PostgreSQL's default lexeme weights. The four variants are:
- [ts_rankcd_wttf](ts_rankcd_wttf.md): weights + tsvector + tsquery + method (full customization)
- [ts_rankcd_wtt](ts_rankcd_wtt.md): weights + tsvector + tsquery (custom weights, default method)
- [ts_rankcd_ttf](ts_rankcd_ttf.md): tsvector + tsquery + method (this function - default weights, custom method)
- [ts_rankcd_tt](ts_rankcd_tt.md): tsvector + tsquery (defaults for both)

By passing NULL to getWeights, this function leverages the default weight scheme while allowing fine-tuning of the normalization approach, making it useful for applications that need to control ranking normalization but don't require custom lexeme category weights.