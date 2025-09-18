# inet_spg_leaf_consistent

## Location
src/backend/utils/adt/network_spgist.c: 323 - 349

## Overview
SP-GiST leaf consistency function for inet/cidr data types that determines whether a leaf tuple matches the search criteria in index queries.

## Definition
```c
Datum inet_spg_leaf_consistent(PG_FUNCTION_ARGS)
```

## Detailed Description
The `inet_spg_leaf_consistent` function is the final consistency check in the SP-GiST query processing pipeline for network addresses. When the index traversal reaches a leaf tuple, this function determines whether the stored network address value satisfies the search conditions specified in the query.

Unlike inner node consistency checking, which only determines which subtrees to visit, leaf consistency performs the actual comparison between the stored network address and the query predicates. The function is designed to be simple and efficient, delegating the complex comparison logic to the shared `inet_spg_consistent_bitmap` helper function.

The function performs exact matches without requiring recheck operations, meaning that any tuple it declares as matching truly satisfies the query conditions. This is possible because network address comparisons are deterministic and don't involve approximate matching or lossy compression.

The function also prepares the leaf value for return to the query executor, ensuring the proper datum format for network addresses.

## Parameters / Member Variables
- `in`: Input structure containing:
  - `leafDatum`: The network address value stored in this leaf tuple
  - `nkeys`: Number of search key conditions
  - `scankeys`: Array of search key conditions with strategy and argument
- `out`: Output structure containing:
  - `recheck`: Whether the tuple needs to be rechecked (always false for inet)
  - `leafValue`: The processed leaf value to return if matched

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetInetPP](../D/DatumGetInetPP.md) (datum to inet conversion)
  - [InetPGetDatum](../I/InetPGetDatum.md) (inet to datum conversion)
  - [inet_spg_consistent_bitmap](inet_spg_consistent_bitmap.md) (evaluate query conditions against address)
  - [spgLeafConsistentIn](../s/spgLeafConsistentIn.md)/spgLeafConsistentOut (SP-GiST structures)
- Called from (representative examples):
  - SP-GiST query processing engine
  - Final stage of index search operations

## Notes and Other Information
- Always sets recheck to false since network address comparisons are exact and deterministic
- Uses the same consistency evaluation logic as inner nodes through inet_spg_consistent_bitmap
- The leaf parameter in inet_spg_consistent_bitmap is set to true to indicate leaf-level processing
- Returns a boolean result directly using PG_RETURN_BOOL macro
- Handles all network address comparison operators (equality, containment, ordering, etc.)
- The function is lightweight and designed for high-frequency execution during index scans
- Proper datum conversion ensures compatibility with PostgreSQL's type system
- No memory allocation is required as it works with existing datum structures