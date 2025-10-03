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
- `pred_op`: OID of the predicate operator
- `clause_op`: OID of the clause operator
- `refute_it`: When false, looks for implication proof; when true, looks for refutation proof
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

## Simplified Source

```c
static OprProofCacheEntry *
lookup_proof_cache(Oid pred_op, Oid clause_op, bool refute_it)
{
    OprProofCacheKey key;
    OprProofCacheEntry *cache_entry;
    bool cfound;
    bool same_subexprs = false;
    Oid test_op = InvalidOid;
    bool found = false;

    // Initialize hash table on first use
    if (OprProofCacheHash == NULL)
    {
        HASHCTL ctl;
        ctl.keysize = sizeof(OprProofCacheKey);
        ctl.entrysize = sizeof(OprProofCacheEntry);
        OprProofCacheHash = hash_create("Btree proof lookup cache", 256, &ctl, HASH_ELEM | HASH_BLOBS);

        // Register callback for cache invalidation
        CacheRegisterSyscacheCallback(AMOPOPID, InvalidateOprProofCacheCallBack, (Datum) 0);
    }

    // Find or create cache entry
    key.pred_op = pred_op;
    key.clause_op = clause_op;
    cache_entry = (OprProofCacheEntry *) hash_search(OprProofCacheHash, &key, HASH_ENTER, &cfound);

    if (!cfound)
    {
        // Initialize new entry
        cache_entry->have_implic = false;
        cache_entry->have_refute = false;
    }
    else
    {
        // Return cached result if available
        if (refute_it ? cache_entry->have_refute : cache_entry->have_implic)
            return cache_entry;
    }

    // Find btree opfamilies containing both operators
    List *clause_op_infos = get_op_btree_interpretation(clause_op);
    List *pred_op_infos = clause_op_infos ? get_op_btree_interpretation(pred_op) : NIL;

    // Search for matching opfamily and determine test operator
    foreach(ListCell *lcp, pred_op_infos)
    {
        OpBtreeInterpretation *pred_op_info = lfirst(lcp);
        Oid opfamily_id = pred_op_info->opfamily_id;

        foreach(ListCell *lcc, clause_op_infos)
        {
            OpBtreeInterpretation *clause_op_info = lfirst(lcc);

            // Must be in same opfamily
            if (opfamily_id != clause_op_info->opfamily_id)
                continue;

            StrategyNumber pred_strategy = pred_op_info->strategy;
            StrategyNumber clause_strategy = clause_op_info->strategy;

            // Check same-subexpressions proof possibility
            if (refute_it)
                same_subexprs |= BT_refutes_table[clause_strategy - 1][pred_strategy - 1];
            else
                same_subexprs |= BT_implies_table[clause_strategy - 1][pred_strategy - 1];

            // Get test strategy from implication tables
            StrategyNumber test_strategy = refute_it ?
                BT_refute_table[clause_strategy - 1][pred_strategy - 1] :
                BT_implic_table[clause_strategy - 1][pred_strategy - 1];

            if (test_strategy == 0)
                continue;

            // Find test operator for this strategy
            if (test_strategy == BTNE)
            {
                test_op = get_opfamily_member(opfamily_id, pred_op_info->oprighttype,
                                            clause_op_info->oprighttype, BTEqualStrategyNumber);
                if (OidIsValid(test_op))
                    test_op = get_negator(test_op);
            }
            else
            {
                test_op = get_opfamily_member(opfamily_id, pred_op_info->oprighttype,
                                            clause_op_info->oprighttype, test_strategy);
            }

            // Verify test operator is immutable
            if (OidIsValid(test_op) && op_volatile(test_op) == PROVOLATILE_IMMUTABLE)
            {
                found = true;
                break;
            }
        }
        if (found) break;
    }

    list_free_deep(pred_op_infos);
    list_free_deep(clause_op_infos);

    // Verify clause operator immutability for same-subexpressions cases
    if (same_subexprs && op_volatile(clause_op) != PROVOLATILE_IMMUTABLE)
        same_subexprs = false;

    // Cache results
    if (refute_it)
    {
        cache_entry->refute_test_op = found ? test_op : InvalidOid;
        cache_entry->same_subexprs_refutes = same_subexprs;
        cache_entry->have_refute = true;
    }
    else
    {
        cache_entry->implic_test_op = found ? test_op : InvalidOid;
        cache_entry->same_subexprs_implies = same_subexprs;
        cache_entry->have_implic = true;
    }

    return cache_entry;
}
```