# ts_rank_wttf

## Location
[src/backend/utils/adt/tsrank.c:438-454](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsrank.c#L438-L454)

## Overview
PostgreSQL SQL function interface for text search ranking that accepts custom weights, TSVector, TSQuery, and normalization method parameters.

## Definition
```c
Datum ts_rank_wttf(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides the PostgreSQL SQL interface for the ts_rank function with custom weights. It serves as the entry point from SQL queries to the internal text search ranking system. The function signature corresponds to the SQL function ts_rank(weights, tsvector, tsquery, normalization_method).

The function extracts arguments from the PostgreSQL function call framework, processes them through the weight validation system (getWeights), and delegates the actual ranking computation to calc_rank. It handles proper memory management by freeing copied arguments and returns the result as a PostgreSQL FLOAT4 datum.

This is the most comprehensive variant of the ts_rank function family, accepting all possible parameters: custom weights for different term categories, the document vector, the search query, and the normalization method flags.

## Parameters / Member Variables
- PG_FUNCTION_ARGS: Standard PostgreSQL function argument structure containing:
  - Argument 0: ArrayType pointer to weight array (weights for D, C, B, A categories)
  - Argument 1: TSVector representing the document
  - Argument 2: TSQuery representing the search query  
  - Argument 3: int32 normalization method flags

## Dependencies
- Functions called/Symbols referenced:
  - PG_DETOAST_DATUM
  - PG_GETARG_TSVECTOR
  - PG_GETARG_TSQUERY
  - PG_GETARG_INT32
  - [calc_rank](../c/calc_rank.md)
  - [getWeights](../g/getWeights.md)
  - PG_FREE_IF_COPY
  - PG_RETURN_FLOAT4
- Called from (representative examples):
  - SQL queries using ts_rank(weights, document, query, method)

## Notes and Other Information
- This is a PostgreSQL C function following the PG_FUNCTION_ARGS convention
- Handles memory detoasting for potentially compressed weight arrays
- Properly manages memory cleanup for all copied arguments
- Returns FLOAT4 (single precision) rather than double precision for efficiency
- Part of PostgreSQL's text search ranking function family (ts_rank variants)
- The 'wttf' suffix indicates the parameter pattern: Weights, TSVector, TSQuery, Flags

## Simplified Source

```c
Datum ts_rank_wttf(PG_FUNCTION_ARGS) {
    ArrayType *win = (ArrayType *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    TSVector txt = PG_GETARG_TSVECTOR(1);
    TSQuery query = PG_GETARG_TSQUERY(2);
    int method = PG_GETARG_INT32(3);
    float res;

    // Calculate ranking with custom weights and normalization method
    res = calc_rank(getWeights(win), txt, query, method);

    // Clean up memory for detoasted arguments
    PG_FREE_IF_COPY(win, 0);
    PG_FREE_IF_COPY(txt, 1);
    PG_FREE_IF_COPY(query, 2);
    PG_RETURN_FLOAT4(res);
}
```