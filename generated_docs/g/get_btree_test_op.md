# get_btree_test_op

## Location
[src/backend/optimizer/util/predtest.c:2330-2345](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/predtest.c#L2330-L2345)

## Overview
Identifies the comparison operator needed for btree-based proofs involving constant comparisons by retrieving the appropriate test operator from the proof cache.

## Definition

```c
static Oid
get_btree_test_op(Oid pred_op, Oid clause_op, bool refute_it)
```
## Detailed Description
This function determines which comparison operator should be used to compare two constants when proving or refuting a predicate based on a clause. Given a true clause "var clause_op const1" and a predicate "var pred_op const2" to prove/refute, it returns the operator needed to compare const2 with const1.

The function acts as a simple accessor to the cached proof information, delegating the complex lookup logic to lookup_proof_cache and returning the appropriate test operator OID from the cache entry.

## Parameters / Member Variables
- `pred_op`: OID of the predicate operator
- `clause_op`: OID of the clause operator
- `refute_it`: When false, returns implication test operator; when true, returns refutation test operator
## Dependencies
- Functions called/Symbols referenced:
  - [lookup_proof_cache](../l/lookup_proof_cache.md)
  - [OprProofCacheEntry](../O/OprProofCacheEntry.md) (accessed fields: refute_test_op, implic_test_op)
- Called from:
  - [operator_predicate_proof](../o/operator_predicate_proof.md)

## Notes and Other Information
This function provides a clean interface for retrieving test operators without exposing cache implementation details. It returns InvalidOid when no suitable comparison operator can be determined for the given operator pair.

## Simplified Source

```c
static Oid
get_btree_test_op(Oid pred_op, Oid clause_op, bool refute_it)
{
    // Look up the cached proof information for these operators
    OprProofCacheEntry *cache_entry = lookup_proof_cache(pred_op, clause_op, refute_it);

    // Return the appropriate test operator based on proof type
    if (refute_it)
        return cache_entry->refute_test_op;    // For refutation proofs
    else
        return cache_entry->implic_test_op;    // For implication proofs
}
```