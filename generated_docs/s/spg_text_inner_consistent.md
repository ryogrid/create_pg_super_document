# spg_text_inner_consistent

## Location
src/backend/access/spgist/spgtextproc.c: 426 - 573

## Overview
The inner consistent function for SP-GiST text operator class that determines which child nodes to visit during index traversal by testing search conditions against reconstructed key values.

## Definition
```c
Datum spg_text_inner_consistent(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is invoked during SP-GiST index scans to determine which child nodes should be traversed. It reconstructs the full key value at each inner node by combining the parent's reconstructed value, any prefix stored at the current node, and the node labels (characters). For each child node, it tests all scan key conditions against the reconstructed value to determine if that subtree could contain matching tuples. The function handles various comparison strategies (equality, less than, greater than, prefix matching) and considers collation rules when appropriate. It returns a list of child nodes that should be visited along with their reconstructed values.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:
  - `in` (spgInnerConsistentIn *): Input structure with scan keys, node information, and reconstruction context
  - `out` (spgInnerConsistentOut *): Output structure to be filled with qualifying child nodes

## Dependencies
- Functions called/Symbols referenced:
  - [spgInnerConsistentIn](spgInnerConsistentIn.md), spgInnerConsistentOut (SP-GiST framework structures)
  - [lc_collate_is_c](../l/lc_collate_is_c.md), PG_GET_COLLATION (collation handling)
  - DatumGetTextPP, DatumGetInt16 (datum conversion functions)
  - SET_VARSIZE, VARDATA (text/varlena manipulation macros)
  - SPG_IS_COLLATION_AWARE_STRATEGY (strategy testing macro)
  - BTLessStrategyNumber, BTEqualStrategyNumber, etc. (comparison strategy constants)
  - [datumCopy](../d/datumCopy.md) (creates deep copy of datum)
- Called from (representative examples):
  - SP-GiST framework during index scans (no direct references found)

## Notes and Other Information
- Handles both collation-aware and non-collation-aware text comparisons
- For non-C collations, may need to traverse the entire subtree due to complex sorting rules
- Reconstructs values incrementally, building upon parent node's reconstructed value
- Supports dummy node labels (values ≤ 0) that don't contribute character data
- Critical for query performance as it determines search tree pruning effectiveness
- The reconstructed value may end with partial multibyte characters, requiring careful handling of encoding-sensitive operations