# ts_rankcd_wttf

## Location
[src/backend/utils/adt/tsrank.c:953-969](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsrank.c#L953-L969)

## Overview
PostgreSQL function that computes the cover density ranking for text search vectors using custom weights, text vector, query, and normalization method.

## Definition
```c
Datum ts_rankcd_wttf(PG_FUNCTION_ARGS)
```

## Detailed Description
`ts_rankcd_wttf` is a PostgreSQL built-in function that calculates the cover density ranking for text search operations. It takes four arguments: a weight array, a tsvector (text search vector), a tsquery (text search query), and a normalization method flag. The function uses the cover density ranking algorithm implemented in `calc_rank_cd` to determine how well the query matches the text vector, taking into account the provided weights and normalization method.

Cover density ranking differs from regular ranking by considering the density of query terms in the text - it gives higher scores to documents where matching terms appear closer together.

## Parameters / Member Variables
- `win` (ArrayType*): Array of weights for different categories of lexemes (A, B, C, D weights)
- `txt` (TSVector): Text search vector containing lexemes and their positions
- `query` (TSQuery): Text search query specifying the terms to match
- `method` (int32): Normalization method flag controlling how the ranking score is normalized

## Dependencies
- Functions called/Symbols referenced:
  - PG_DETOAST_DATUM: Decompresses toasted (large) data if needed
  - PG_GETARG_TSVECTOR: Extracts tsvector argument from function call
  - PG_GETARG_TSQUERY: Extracts tsquery argument from function call
  - PG_GETARG_INT32: Extracts int32 argument from function call
  - [getWeights](../g/getWeights.md): Processes the weight array into usable format
  - [calc_rank_cd](../c/calc_rank_cd.md): Core cover density ranking calculation function
  - PG_FREE_IF_COPY: Frees memory for copied arguments if needed
  - PG_RETURN_FLOAT4: Returns float4 result to PostgreSQL
- Called from (representative examples):
  - None directly (SQL function interface)

## Notes and Other Information
This function serves as a PostgreSQL SQL-callable wrapper around the core cover density ranking algorithm. It is one of four ts_rankcd variants that differ in their parameter signatures:
- [ts_rankcd_wttf](ts_rankcd_wttf.md): weights + tsvector + tsquery + method
- [ts_rankcd_wtt](ts_rankcd_wtt.md): weights + tsvector + tsquery (uses default method)  
- [ts_rankcd_ttf](ts_rankcd_ttf.md): tsvector + tsquery + method (uses default weights)
- [ts_rankcd_tt](ts_rankcd_tt.md): tsvector + tsquery (uses defaults for both)

The function handles memory management for potentially large arguments through PostgreSQL's detoasting and copy mechanisms.