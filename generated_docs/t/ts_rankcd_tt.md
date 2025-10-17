# ts_rankcd_tt

## Location
[src/backend/utils/adt/tsrank.c:1001-1012](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsrank.c#L1001-L1012)

## Overview
PostgreSQL function that computes the cover density ranking for text search vectors using default weights and normalization method with text vector and query.

## Definition
```c
Datum ts_rankcd_tt(PG_FUNCTION_ARGS)
```

## Detailed Description
`ts_rankcd_tt` is a PostgreSQL built-in function that calculates the cover density ranking for text search operations using all default parameters. It takes only two arguments: a tsvector (text search vector) and a tsquery (text search query), using both default weights and the default normalization method (DEF_NORM_METHOD). This is the simplest variant of the ts_rankcd family, providing basic cover density ranking functionality without any customization options.

This function is ideal for users who want cover density ranking with standard PostgreSQL behavior, focusing purely on the text content and query without needing to tune weights or normalization parameters.

## Parameters / Member Variables
- `txt` (TSVector): Text search vector containing lexemes and their positions
- `query` (TSQuery): Text search query specifying the terms to match

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TSVECTOR: Extracts tsvector argument from function call
  - PG_GETARG_TSQUERY: Extracts tsquery argument from function call
  - [getWeights](../g/getWeights.md): Processes NULL to return default weight values
  - [calc_rank_cd](../c/calc_rank_cd.md): Core cover density ranking calculation function
  - DEF_NORM_METHOD: Default normalization method constant
  - PG_FREE_IF_COPY: Frees memory for copied arguments if needed
  - PG_RETURN_FLOAT4: Returns float4 result to PostgreSQL
- Called from (representative examples):
  - None directly (SQL function interface)

## Notes and Other Information
This function represents the most basic and commonly used variant of the ts_rankcd family. It is the entry point for users who want to use cover density ranking without needing to understand or configure the underlying weight and normalization parameters. The complete family includes:
- [ts_rankcd_wttf](ts_rankcd_wttf.md): weights + tsvector + tsquery + method (full customization)
- [ts_rankcd_wtt](ts_rankcd_wtt.md): weights + tsvector + tsquery (custom weights, default method)
- [ts_rankcd_ttf](ts_rankcd_ttf.md): tsvector + tsquery + method (default weights, custom method)
- [ts_rankcd_tt](ts_rankcd_tt.md): tsvector + tsquery (this function - defaults for both)

The function provides the fastest execution path among the variants since it avoids parameter processing overhead for weights and normalization method. It's particularly suitable for applications that need reliable, consistent ranking behavior across different queries without requiring fine-tuning of ranking parameters.

## Simplified Source

```c
Datum
ts_rankcd_tt(PG_FUNCTION_ARGS)
{
    // Extract function arguments
    TSVector txt = PG_GETARG_TSVECTOR(0);
    TSQuery query = PG_GETARG_TSQUERY(1);
    float res;

    // Calculate cover density ranking with all defaults
    res = calc_rank_cd(getWeights(NULL), txt, query, DEF_NORM_METHOD);

    // Clean up memory and return result
    PG_FREE_IF_COPY(txt, 0);
    PG_FREE_IF_COPY(query, 1);
    PG_RETURN_FLOAT4(res);
}
```