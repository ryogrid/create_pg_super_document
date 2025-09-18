# ts_rankcd_wtt

## Location
src/backend/utils/adt/tsrank.c: 970 - 985

## Overview
PostgreSQL function that computes the cover density ranking for text search vectors using custom weights, text vector, and query with default normalization method.

## Definition
```c
Datum ts_rankcd_wtt(PG_FUNCTION_ARGS)
```

## Detailed Description
`ts_rankcd_wtt` is a PostgreSQL built-in function that calculates the cover density ranking for text search operations. It takes three arguments: a weight array, a tsvector (text search vector), and a tsquery (text search query), using the default normalization method (DEF_NORM_METHOD). This function is a convenience wrapper that provides default normalization while allowing custom weights to be specified.

The cover density ranking algorithm considers not just term frequency but also the proximity of matching terms, giving higher scores to documents where query terms appear closer together.

## Parameters / Member Variables
- `win` (ArrayType*): Array of weights for different categories of lexemes (A, B, C, D weights)
- `txt` (TSVector): Text search vector containing lexemes and their positions
- `query` (TSQuery): Text search query specifying the terms to match

## Dependencies
- Functions called/Symbols referenced:
  - PG_DETOAST_DATUM: Decompresses toasted (large) data if needed
  - PG_GETARG_TSVECTOR: Extracts tsvector argument from function call
  - PG_GETARG_TSQUERY: Extracts tsquery argument from function call
  - getWeights: Processes the weight array into usable format
  - calc_rank_cd: Core cover density ranking calculation function
  - DEF_NORM_METHOD: Default normalization method constant
  - PG_FREE_IF_COPY: Frees memory for copied arguments if needed
  - PG_RETURN_FLOAT4: Returns float4 result to PostgreSQL
- Called from (representative examples):
  - None directly (SQL function interface)

## Notes and Other Information
This function is one of four ts_rankcd variants that provide different parameter combinations:
- ts_rankcd_wttf: weights + tsvector + tsquery + method (full control)
- ts_rankcd_wtt: weights + tsvector + tsquery (this function - uses default method)
- ts_rankcd_ttf: tsvector + tsquery + method (uses default weights)
- ts_rankcd_tt: tsvector + tsquery (uses defaults for both)

By using DEF_NORM_METHOD, this variant provides a balance between customization (custom weights) and convenience (default normalization), making it suitable for applications that need to tune lexeme weights but are satisfied with the standard normalization approach.