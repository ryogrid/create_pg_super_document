# ts_rank_tt

## Location
[src/backend/utils/adt/tsrank.c:486-515](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsrank.c#L486-L515)

## Overview
A PostgreSQL function that calculates the rank of a TSVector against a TSQuery using default weights and default normalization method.

## Definition

```c
typedef struct
{
	union
	{
		struct
		{						/* compiled doc representation */
			QueryItem **items;
			int16		nitem;
		}			query;
		struct
		{						/* struct is used for preparing doc
								 * representation */
			QueryItem  *item;
			WordEntry  *entry;
		}			map;
	}			data;
	WordEntryPos pos;
} DocRepresentation;
```
## Detailed Description
The  function is the simplest PostgreSQL built-in text search ranking function. It takes only two arguments: a TSVector representing indexed text and a TSQuery representing the search criteria. This function uses both default weights for the four word classes (D, C, B, A) and the default normalization method (RANK_NO_NORM). It provides the most straightforward way to calculate text search ranking scores without any customization options, making it the most commonly used variant for basic full-text search applications.

## Parameters / Member Variables
-  (TSVector): The text search vector representing the indexed document content
-  (TSQuery): The text search query expression to match against the TSVector

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TSVECTOR: Extracts TSVector from function arguments
  - PG_GETARG_TSQUERY: Extracts TSQuery from function arguments
  - [getWeights](../g/getWeights.md): Called with NULL to use default weight values for all word classes
  - [calc_rank](../c/calc_rank.md): Performs the actual ranking calculation with default settings
  - DEF_NORM_METHOD: Default normalization method constant (RANK_NO_NORM)
  - PG_FREE_IF_COPY: Frees detoasted copies of arguments
  - PG_RETURN_FLOAT4: Returns the calculated rank as a float4 value
- Called from (representative examples):
  - SQL queries using ts_rank(tsvector, tsquery) function

## Notes and Other Information
- This is the most basic ts_rank variant, using all default settings
- Uses default weights for all word classes, treating D, C, B, and A classes with standard importance
- Applies no normalization to the final ranking score (RANK_NO_NORM)
- Ideal for simple full-text search scenarios where customization is not needed
- Part of PostgreSQL's full-text search system alongside ts_rank_wtt (custom weights) and ts_rank_ttf (custom normalization)
- The function automatically handles memory management by freeing detoasted argument copies
- Provides the foundation for more complex ranking calculations in other ts_rank variants

## Simplified Source

```c
Datum ts_rank_tt(PG_FUNCTION_ARGS) {
    TSVector txt = PG_GETARG_TSVECTOR(0);
    TSQuery query = PG_GETARG_TSQUERY(1);
    float res;

    // Calculate ranking with default weights and default normalization
    res = calc_rank(getWeights(NULL), txt, query, DEF_NORM_METHOD);

    // Clean up memory for detoasted arguments
    PG_FREE_IF_COPY(txt, 0);
    PG_FREE_IF_COPY(query, 1);
    PG_RETURN_FLOAT4(res);
}
```