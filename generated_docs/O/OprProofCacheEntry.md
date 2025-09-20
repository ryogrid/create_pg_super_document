# OprProofCacheEntry

## Location
[src/backend/optimizer/util/predtest.c:2080-2091](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/predtest.c#L2080-L2091)

## Overview
OprProofCacheEntry is a hash table entry structure that stores cached results of B-tree operator proof lookups in PostgreSQL's predicate testing system, containing both the lookup key and the computed logical relationship results for operator pairs.

## Definition

```c
typedef struct OprProofCacheEntry
{
	/* the hash lookup key MUST BE FIRST */
	OprProofCacheKey key;

	bool		have_implic;	/* do we know the implication result? */
	bool		have_refute;	/* do we know the refutation result? */
	bool		same_subexprs_implies;	/* X clause_op Y implies X pred_op Y? */
	bool		same_subexprs_refutes;	/* X clause_op Y refutes X pred_op Y? */
	Oid			implic_test_op; /* OID of the test operator, or 0 if none */
	Oid			refute_test_op; /* OID of the test operator, or 0 if none */
} OprProofCacheEntry;
```
## Detailed Description
OprProofCacheEntry represents a complete cache entry in the B-tree operator proof lookup system. This structure stores both the lookup key (operator pair) and all the computed results about the logical relationships between those operators. The cache serves to avoid expensive repeated lookups in the system catalogs (particularly pg_amop) when determining whether one operator implies or refutes another.

The entry tracks two types of logical relationships: same-subexpressions proofs (where the operands are identical) and test operator proofs (where constants need to be compared using auxiliary operators). For each operator pair, the system can determine both implication relationships (when one operator logically implies another) and refutation relationships (when one operator logically contradicts another). The cache lazily computes these relationships as needed and marks which results have been determined.

## Parameters / Member Variables
- : OprProofCacheKey containing the predicate and clause operator OIDs that identify this cache entry
- : Boolean flag indicating whether the implication relationship has been computed and cached
- : Boolean flag indicating whether the refutation relationship has been computed and cached
- : Result for same-subexpressions implication test (e.g., 'X > Y' implies 'X ≥ Y')
- : Result for same-subexpressions refutation test (e.g., 'X = Y' refutes 'X ≠ Y')
- : OID of the operator used for constant comparison in implication proofs, or InvalidOid if none
- : OID of the operator used for constant comparison in refutation proofs, or InvalidOid if none

## Dependencies
- Functions called/Symbols referenced:
  - [OprProofCacheKey](OprProofCacheKey.md) (the hash key structure)
  - Oid (PostgreSQL object identifier type)
- Called from (representative examples):
  - [lookup_proof_cache](../l/lookup_proof_cache.md) (creates and retrieves cache entries)
  - [operator_same_subexprs_lookup](../o/operator_same_subexprs_lookup.md) (accesses cached same-subexpressions results)
  - [get_btree_test_op](../g/get_btree_test_op.md) (accesses cached test operator results)
  - [InvalidateOprProofCacheCallBack](../I/InvalidateOprProofCacheCallBack.md) (resets cache entries when catalogs change)

## Notes and Other Information
- The key field must be first to serve as the hash table lookup key (requirement of PostgreSQL's hash table implementation)
- Cache entries are invalidated when pg_amop catalog changes are detected via syscache callbacks
- The cache supports partial computation - implication and refutation results can be computed independently
- Used extensively in predicate_implied_by and predicate_refuted_by operations during query optimization
- Significantly improves performance by avoiding repeated catalog lookups for the same operator pairs
- Test operators are used when comparing constants requires a different operator than the original (e.g., using '<' to test if 'a > 5' implies 'a > 3')