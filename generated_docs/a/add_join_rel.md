# add_join_rel

## Location
[src/backend/optimizer/util/relnode.c:627-664](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/relnode.c#L627-L664)

## Overview
Adds a given join relation to the list of join relations in the PlannerInfo structure and also adds it to the auxiliary hashtable if one exists.

## Definition
static void add_join_rel(PlannerInfo *root, RelOptInfo *joinrel)

## Detailed Description
This static function performs the essential task of registering a newly created join relation with the planner's data structures. It maintains two data structures for join relations: a list (join_rel_list) and an optional hash table (join_rel_hash). The function first appends the join relation to the end of the join_rel_list using lappend, which is specifically required by GEQO (Genetic Query Optimization).

If a join relation hash table exists, the function also inserts the join relation into the hash table for fast lookup. It uses the joinrel's relids as the hash key and stores the RelOptInfo pointer in a JoinHashEntry structure. The function includes an assertion to verify that no duplicate entry exists in the hash table, ensuring data integrity.

## Parameters / Member Variables
- : PlannerInfo structure containing the join relation list and optional hash table
- : The RelOptInfo structure for the join relation to be added

## Dependencies
- Functions called/Symbols referenced:
  - lappend (appends to list)
  - [JoinHashEntry](../J/JoinHashEntry.md) (hash table entry structure)
  - [hash_search](../h/hash_search.md) (performs hash table insertion)
  - HASH_ENTER (hash operation flag for insertion)
  - Assert (debugging assertion)
- Called from (representative examples):
  - build_join_rel
  - build_child_join_rel

## Notes and Other Information
- This is a static function, only used internally within relnode.c
- GEQO specifically requires appending to the end of the list rather than prepending
- Uses assertion to ensure no duplicate entries are added to the hash table
- Maintains both list and hash table data structures for different access patterns
- Hash table insertion is conditional based on whether the hash table exists
- Located in src/backend/optimizer/util/relnode.c:627-664