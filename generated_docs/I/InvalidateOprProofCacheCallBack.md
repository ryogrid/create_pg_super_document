# InvalidateOprProofCacheCallBack

## Location
src/backend/optimizer/util/predtest.c: 2346 - 2361

## Overview
This is a callback function that invalidates the operator proof cache entries when the pg_amop catalog is changed, ensuring cache consistency with the underlying system catalogs.

## Definition
static void InvalidateOprProofCacheCallBack(Datum arg, int cacheid, uint32 hashvalue)

## Detailed Description
InvalidateOprProofCacheCallBack is a static callback function designed to handle invalidation events from the PostgreSQL catalog cache system, specifically for pg_amop (access method operator) changes. When the pg_amop catalog is modified (such as when operators are added, removed, or modified), this callback is triggered to maintain cache consistency.

The function implements a simple but effective invalidation strategy: it resets all entries in the OprProofCacheHash rather than attempting selective invalidation. This approach ensures correctness at the cost of some performance, as indicated by the comment "hard to be smarter". The function iterates through all cache entries and marks them as invalid by setting both have_implic and have_refute flags to false.

## Parameters / Member Variables
- `arg`: Datum argument passed by the callback system (not used in this implementation)
- `cacheid`: Integer identifier for the cache that triggered this callback
- `hashvalue`: Hash value associated with the invalidated cache entry (not used in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)
  - HASH_SEQ_STATUS (type)
  - [OprProofCacheEntry](../O/OprProofCacheEntry.md) (type)
- Called from (representative examples):
  - iterate_end (predtest.c:112)
  - [lookup_proof_cache](../l/lookup_proof_cache.md) (predtest.c:2129)

## Notes and Other Information
- This function is part of PostgreSQL's predicate testing subsystem located in predtest.c
- The function assumes OprProofCacheHash is not NULL (enforced by Assert)
- Uses a "reset all" strategy rather than selective invalidation for simplicity and correctness
- The cache entries maintain two boolean flags (have_implic, have_refute) that track whether implication and refutation proofs have been computed
- This callback ensures that cached operator proof information remains consistent when the underlying operator definitions change