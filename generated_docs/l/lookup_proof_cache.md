# lookup_proof_cache

## Location
src/backend/optimizer/util/predtest.c: 2101 - 2304

## Overview
Retrieves and populates cache entries for operator proof relationships, analyzing btree operator families to determine logical implications and test operators for constant comparisons.

## Definition


## Detailed Description
This function manages a hash-based cache for storing operator proof relationships. It searches for btree operator families containing both the predicate and clause operators, then uses strategy tables (BT_implies_table, BT_refutes_table, etc.) to determine logical relationships. For constant comparison cases, it identifies appropriate test operators and verifies their immutability.

The function initializes the hash table on first use and registers a syscache callback to invalidate the cache when pg_amop changes. It handles both implication and refutation cases, caching results to avoid repeated lookups for the same operator pairs.

## Parameters / Member Variables
- : OID of the predicate operator
- : OID of the clause operator  
- : When false, looks for implication proof; when true, looks for refutation proof

## Dependencies
- Functions called/Symbols referenced:
  - hash_create
  - hash_search
  - CacheRegisterSyscacheCallback
  - InvalidateOprProofCacheCallBack
  - get_op_btree_interpretation
  - get_opfamily_member
  - get_negator
  - op_volatile
  - list_free_deep
- Called from:
  - operator_same_subexprs_lookup
  - get_btree_test_op

## Notes and Other Information
The cache uses OprProofCacheKey (pred_op, clause_op) as the key and stores separate flags for implication and refutation proofs. The function requires both operators to be in the same btree opfamily and verifies that test operators are immutable. It handles special cases like BTNE (not-equal) strategy by finding the equality operator and its negator.