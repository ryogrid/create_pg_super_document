# ts_rank_wtt

## Location
[src/backend/utils/adt/tsrank.c:455-470](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsrank.c#L455-L470)

## Overview
A PostgreSQL function that calculates the rank of a TSVector (text search vector) against a TSQuery with custom weight assignments for different word classes.

## Definition

```c
Datum
ts_rank_wtt(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in function that computes text search ranking scores. It takes three arguments: a weight array, a TSVector representing indexed text, and a TSQuery representing the search criteria. The function uses custom weights provided in the array parameter to assign different importance levels to the four word classes (D, C, B, A) in PostgreSQL's text search system. The ranking calculation is performed using the default normalization method (RANK_NO_NORM), meaning no normalization is applied to the final score.

## Parameters / Member Variables
-  (ArrayType*): Array of weight values for the four word classes (D, C, B, A). Must be a one-dimensional array with at least 4 float values between 0 and 1
-  (TSVector): The text search vector representing the indexed document content
-  (TSQuery): The text search query expression to match against the TSVector

## Dependencies
- Functions called/Symbols referenced:
  - PG_DETOAST_DATUM: Decompresses the weight array argument if needed
  - PG_GETARG_TSVECTOR: Extracts TSVector from function arguments
  - PG_GETARG_TSQUERY: Extracts TSQuery from function arguments
  - [getWeights](../g/getWeights.md): Validates and extracts weight values from the array
  - [calc_rank](../c/calc_rank.md): Performs the actual ranking calculation
  - DEF_NORM_METHOD: Default normalization method constant (RANK_NO_NORM)
  - PG_FREE_IF_COPY: Frees detoasted copies of arguments
  - PG_RETURN_FLOAT4: Returns the calculated rank as a float4 value
- Called from (representative examples):
  - SQL queries using ts_rank(weights, tsvector, tsquery) function

## Notes and Other Information
- This is one of the PostgreSQL text search ranking functions, specifically the variant that accepts custom weights
- The weight array allows fine-tuning of how different word classes contribute to the final ranking score
- The function automatically handles memory management by freeing detoasted argument copies
- Uses the default normalization method which applies no normalization to the calculated rank
- The rank calculation internally delegates to calc_rank() which handles both AND/PHRASE and OR query operations differently

## Simplified Source

```c
Datum ts_rank_wtt(PG_FUNCTION_ARGS) {
    ArrayType *win = (ArrayType *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    TSVector txt = PG_GETARG_TSVECTOR(1);
    TSQuery query = PG_GETARG_TSQUERY(2);
    float res;

    // Calculate ranking with custom weights and default normalization
    res = calc_rank(getWeights(win), txt, query, DEF_NORM_METHOD);

    // Clean up memory for detoasted arguments
    PG_FREE_IF_COPY(win, 0);
    PG_FREE_IF_COPY(txt, 1);
    PG_FREE_IF_COPY(query, 2);
    PG_RETURN_FLOAT4(res);
}
```