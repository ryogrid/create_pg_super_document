# lookup_proof_cache

## Location
[src/backend/optimizer/util/predtest.c:2101-2304](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/predtest.c#L2101-L2304)

## Overview
Retrieves and populates cache entries for operator proof relationships, analyzing btree operator families to determine logical implications and test operators for constant comparisons.

## Definition

```c
static OprProofCacheEntry *
lookup_proof_cache(Oid pred_op, Oid clause_op, bool refute_it)
```
## Detailed Description
This function manages a hash-based cache for storing operator proof relationships. It searches for btree operator families containing both the predicate and clause operators, then uses strategy tables (BT_implies_table, BT_refutes_table, etc.) to determine logical relationships. For constant comparison cases, it identifies appropriate test operators and verifies their immutability.

The function initializes the hash table on first use and registers a syscache callback to invalidate the cache when pg_amop changes. It handles both implication and refutation cases, caching results to avoid repeated lookups for the same operator pairs.

## Parameters / Member Variables
- : OID of the predicate operator
- : OID of the clause operator  
- : When false, looks for implication proof; when true, looks for refutation proof

## Dependencies
- Functions called/Symbols referenced:
  - [hash_create](../h/hash_create.md)
  - [hash_search](../h/hash_search.md)
  - [CacheRegisterSyscacheCallback](../C/CacheRegisterSyscacheCallback.md)
  - [InvalidateOprProofCacheCallBack](../I/InvalidateOprProofCacheCallBack.md)
  - [get_op_btree_interpretation](../g/get_op_btree_interpretation.md)
  - [get_opfamily_member](../g/get_opfamily_member.md)
  - [get_negator](../g/get_negator.md)
  - [op_volatile](../o/op_volatile.md)
  - [list_free_deep](list_free_deep.md)
- Called from:
  - [operator_same_subexprs_lookup](../o/operator_same_subexprs_lookup.md)
  - [get_btree_test_op](../g/get_btree_test_op.md)

## Notes and Other Information
The cache uses OprProofCacheKey (pred_op, clause_op) as the key and stores separate flags for implication and refutation proofs. The function requires both operators to be in the same btree opfamily and verifies that test operators are immutable. It handles special cases like BTNE (not-equal) strategy by finding the equality operator and its negator.