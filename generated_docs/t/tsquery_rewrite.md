# tsquery_rewrite

## Location
src/backend/utils/adt/tsquery_rewrite.c: 410 - 462

## Overview
The `tsquery_rewrite` function implements a simple single-rule TSQuery rewriting operation, applying one specific pattern-replacement transformation to an input query.

## Definition
```c
Datum tsquery_rewrite(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the three-argument version of the `ts_rewrite()` SQL function, which performs a straightforward single-step rewrite operation on a TSQuery. Unlike the query-based version, this function takes explicit pattern and replacement TSQuery arguments and applies the transformation directly.

The function operates by:
1. Extracting three TSQuery arguments: the input query, the pattern to match, and the replacement pattern
2. Converting all TSQuery inputs to internal tree representations (QTNode)
3. Preprocessing all trees with ternary conversion and sorting for reliable pattern matching
4. Applying the single rewrite rule using `findsubquery`
5. Converting the result back to TSQuery format or returning an empty query if completely eliminated

This function provides a simpler interface compared to `tsquery_rewrite_query` for cases where only a single, well-defined rewrite rule needs to be applied. It's more efficient for simple transformations as it avoids the overhead of SQL query execution and result processing.

## Parameters / Member Variables
- Function follows PostgreSQL SQL function convention with `PG_FUNCTION_ARGS`
- `query`: Input TSQuery to be rewritten (argument 0)
- `ex`: Pattern TSQuery to search for (argument 1)  
- `subst`: Replacement TSQuery to substitute matches (argument 2)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TSQUERY_COPY, PG_GETARG_TSQUERY (argument extraction)
  - QT2QTN (TSQuery to tree conversion)
  - QTNTernary, QTNSort (tree preprocessing)
  - findsubquery (pattern replacement)
  - QTNFree (memory cleanup)
  - QTNBinary, QTN2QT (tree to TSQuery conversion)
  - SET_VARSIZE, HDRSIZETQ (empty query handling)
- Called from (representative examples):
  - SQL queries using ts_rewrite(tsquery, tsquery, tsquery) function

## Notes and Other Information
- Implements the three-argument version of ts_rewrite() SQL function
- More efficient than the query-based version for simple single-rule transformations
- Handles edge cases like empty input queries or patterns gracefully
- Returns an empty TSQuery if the entire query is eliminated by the rewrite rule
- Proper memory management with cleanup of intermediate tree structures
- No iterative processing - applies the rule exactly once
- Suitable for simple, deterministic query transformations where the pattern and replacement are known in advance