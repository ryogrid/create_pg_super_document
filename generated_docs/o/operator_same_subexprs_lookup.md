# operator_same_subexprs_lookup

## Location
[src/backend/optimizer/util/predtest.c:2305-2329](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/predtest.c#L2305-L2329)

## Overview
Convenience function that retrieves cached proof results for same-subexpressions cases by delegating to the lookup_proof_cache and extracting the appropriate boolean result.

## Definition

```c
static bool
operator_same_subexprs_lookup(Oid pred_op, Oid clause_op, bool refute_it)
```
## Detailed Description
This is a simple wrapper function that calls lookup_proof_cache to get the cached proof entry for a given operator pair, then extracts the appropriate boolean field based on whether this is an implication or refutation test. It provides a clean interface for checking same-subexpressions proofs without exposing the cache entry structure to callers.

## Parameters / Member Variables
- : OID of the predicate operator
- : OID of the clause operator
- : When false, returns implication result; when true, returns refutation result

## Dependencies
- Functions called/Symbols referenced:
  - [lookup_proof_cache](../l/lookup_proof_cache.md)
  - [OprProofCacheEntry](../O/OprProofCacheEntry.md) (accessed fields: same_subexprs_refutes, same_subexprs_implies)
- Called from:
  - [operator_same_subexprs_proof](operator_same_subexprs_proof.md)

## Notes and Other Information
This function serves as an abstraction layer over the cache lookup mechanism, simplifying access to cached proof results for the common case of same-subexpression operator relationships.