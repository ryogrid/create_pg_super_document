# spg_text_leaf_consistent

## Location
[src/backend/access/spgist/spgtextproc.c:574-701](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgtextproc.c#L574-L701)

## Overview
The leaf consistent function for SP-GiST text operator class that tests search conditions against actual stored text values at leaf nodes to determine if they match the query.

## Definition
```c
Datum spg_text_leaf_consistent(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is invoked during SP-GiST index scans when reaching leaf nodes. It reconstructs the complete text value by combining the reconstructed value from inner nodes with the leaf tuple's stored suffix. The function then performs exact comparisons between this reconstructed full text value and the search query according to the specified strategy (equality, less than, greater than, prefix matching). For prefix queries, it can optimize by checking if the reconstructed inner node value already satisfies the prefix condition. The function handles both collation-aware and non-collation-aware comparisons and returns a boolean result indicating whether the leaf tuple matches the search conditions.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:
  - `in` (spgLeafConsistentIn *): Input structure with leaf tuple data, scan keys, and reconstruction context
  - `out` (spgLeafConsistentOut *): Output structure to be filled with match result and reconstructed leaf value

## Dependencies
- Functions called/Symbols referenced:
  - [spgLeafConsistentIn](spgLeafConsistentIn.md), spgLeafConsistentOut (SP-GiST framework structures)
  - DatumGetTextPP (text datum conversion)
  - SET_VARSIZE, VARDATA (text/varlena manipulation macros)
  - [text_starts_with](../t/text_starts_with.md), DirectFunctionCall2Coll (prefix comparison functions)
  - PG_GET_COLLATION (collation context)
  - SPG_IS_COLLATION_AWARE_STRATEGY (strategy testing macro)
  - [pg_verifymbstr](../p/pg_verifymbstr.md) (multibyte string validation)
  - [varstr_cmp](../v/varstr_cmp.md) (collation-aware string comparison)
  - BTLessStrategyNumber, BTEqualStrategyNumber, etc. (comparison strategy constants)
- Called from (representative examples):
  - SP-GiST framework during leaf node evaluation (no direct references found)

## Notes and Other Information
- Sets recheck to false since all tests are exact and no post-filtering is needed
- Handles the special case where leaf value is empty but level > 0 by reusing the reconstructed value
- For prefix queries, can short-circuit when the reconstructed inner value already contains the full query prefix
- Supports both collation-aware and byte-wise string comparisons depending on the strategy
- Validates multibyte string encoding in debug builds for collation-aware comparisons
- Critical for final result accuracy as it performs the definitive match test against actual stored values