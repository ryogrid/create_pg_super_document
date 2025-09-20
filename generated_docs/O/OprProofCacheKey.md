# OprProofCacheKey

## Location
[src/backend/optimizer/util/predtest.c:2074-2078](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/predtest.c#L2074-L2078)

## Overview
OprProofCacheKey is a structure that serves as the hash table key for caching B-tree operator proof lookup results in PostgreSQL's predicate testing system, improving performance by avoiding repeated expensive operator relationship queries.

## Definition

```c
typedef struct OprProofCacheKey
{
	Oid			pred_op;		/* predicate operator */
	Oid			clause_op;		/* clause operator */
} OprProofCacheKey;
```
## Detailed Description
OprProofCacheKey is used as the lookup key in a hash table that caches the results of B-tree proof operator lookups in PostgreSQL's optimizer. The predicate testing system needs to determine relationships between operators (such as whether one operator implies or refutes another) by examining the B-tree operator families in the system catalogs. Since these lookups are expensive and the relationships don't change during a session (unless pg_amop is modified), this caching mechanism significantly improves performance.

The cache stores both implication and refutation results for each operator pair. For example, it might cache that the '>' operator implies the '≥' operator when applied to the same operands, or that the '=' operator refutes the '≠' operator. This information is crucial for query optimization, particularly in determining when WHERE clause conditions can be simplified or when indexes can be used effectively.

## Parameters / Member Variables
- : OID of the predicate operator (the operator being tested for logical relationships)
- : OID of the clause operator (the operator that might imply or refute the predicate operator)

## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL object identifier type)
- Called from (representative examples):
  - [OprProofCacheEntry](OprProofCacheEntry.md) (used as the key field in cache entries)
  - [lookup_proof_cache](../l/lookup_proof_cache.md) (constructs keys for cache lookups)

## Notes and Other Information
- This structure must be the first field in OprProofCacheEntry to serve as the hash table key
- The cache is invalidated when pg_amop (access method operator) catalog changes are detected
- Used in conjunction with PostgreSQL's hash table infrastructure (hash_create, hash_search)
- Part of the btree proof lookup optimization that speeds up predicate implication and refutation testing
- The key uniquely identifies an operator pair relationship that can be cached across multiple query optimizations within a session