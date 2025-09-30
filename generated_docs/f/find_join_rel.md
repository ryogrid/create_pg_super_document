# find_join_rel

## Location
[src/backend/optimizer/util/relnode.c:527-588](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/relnode.c#L527-L588)

## Overview
Returns the relation entry corresponding to a set of RT indexes (relids), or NULL if none exists, specifically for join relations.

## Definition
RelOptInfo *find_join_rel(PlannerInfo *root, Relids relids)

## Detailed Description
This function provides efficient lookup of join relations using their relation identifier sets (Relids). It implements an adaptive search strategy that switches between linear search and hash table lookup based on the size of the join_rel_list. When the list contains more than 32 entries, the function automatically builds a hash table (via build_join_rel_hash) to enable faster O(1) lookups instead of O(n) linear searches.

The function first checks if a hash table exists and if the join relation list has grown beyond the threshold of 32 entries. If so, it builds the hash table. Then it uses either hash table lookup or linear search depending on whether the hash table is available. For hash table lookup, it searches for a JoinHashEntry with the matching relids. For linear search, it iterates through the join_rel_list and uses bms_equal to compare relids. The function includes an optimization note about using a hashkey variable to avoid forcing relids out of registers during compilation.

## Parameters / Member Variables
- : PlannerInfo structure containing the join relation list and optional hash table
- : Set of RT (Range Table) indexes identifying the join relation to find

## Dependencies
- Functions called/Symbols referenced:
  - [build_join_rel_hash](../b/build_join_rel_hash.md) (builds hash table when needed)
  - [JoinHashEntry](../J/JoinHashEntry.md) (hash table entry structure)
  - [hash_search](../h/hash_search.md) (performs hash table lookup)
  - HASH_FIND (hash operation flag)
  - [bms_equal](../b/bms_equal.md) (compares two Relids bitmapsets)
  - [list_length](../l/list_length.md) (gets length of join_rel_list)
- Called from (representative examples):
  - [get_matching_part_pairs](../g/get_matching_part_pairs.md)
  - [build_join_rel](../b/build_join_rel.md)
  - build_child_join_rel
  - [examine_variable](../e/examine_variable.md)
  - [find_join_input_rel](find_join_input_rel.md)

## Notes and Other Information
- Implements adaptive search strategy: linear search for small lists (≤32 entries), hash table for larger lists
- The threshold of 32 entries is arbitrary and known only within this function
- Uses hashkey variable optimization to avoid register pressure during compilation
- Returns NULL if no matching join relation is found
- [Hash](../H/Hash.md) table is built lazily only when needed
- Located in src/backend/optimizer/util/relnode.c:527-588

## Simplified Source

```c
RelOptInfo *
find_join_rel(PlannerInfo *root, Relids relids)
{
    // Build hash table when join list grows beyond 32 entries
    if (!root->join_rel_hash && list_length(root->join_rel_list) > 32)
        build_join_rel_hash(root);

    // Use hash lookup if available, otherwise linear search
    if (root->join_rel_hash) {
        // Hash table lookup - O(1) performance
        Relids hashkey = relids;  // Avoid register pressure
        JoinHashEntry *hentry = (JoinHashEntry *) hash_search(root->join_rel_hash,
                                                              &hashkey,
                                                              HASH_FIND,
                                                              NULL);
        if (hentry)
            return hentry->join_rel;
    } else {
        // Linear search through join relation list - O(n) performance
        ListCell *l;
        foreach(l, root->join_rel_list) {
            RelOptInfo *rel = (RelOptInfo *) lfirst(l);
            if (bms_equal(rel->relids, relids))
                return rel;
        }
    }

    return NULL;  // No matching join relation found
}
```