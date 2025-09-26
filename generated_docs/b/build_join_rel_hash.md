# build_join_rel_hash

## Location
[src/backend/optimizer/util/relnode.c:486-526](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/relnode.c#L486-L526)

## Overview
Constructs the auxiliary hash table for join relations to enable fast lookup of existing join relations by their relation identifier sets.

## Definition
static void build_join_rel_hash(PlannerInfo *root)

## Detailed Description
This static function creates and populates a hash table that serves as an auxiliary data structure for quickly finding join relations. The hash table uses Relids (bitmap of relation identifiers) as keys and stores pointers to the corresponding RelOptInfo structures for join relations.

The function first creates a hash table with specific configuration: it uses bitmap_hash for hashing Relids keys, bitmap_match for key comparison, and sets up appropriate entry sizes. The initial size is set to 256 entries. After creating the hash table, the function iterates through all existing join relations in root->join_rel_list and inserts them into the hash table, using each relation's relids as the key and storing a pointer to the RelOptInfo in the hash entry.

## Parameters / Member Variables
- : PlannerInfo structure that will have its join_rel_hash field populated with the newly created hash table

## Dependencies
- Functions called/Symbols referenced:
  - [HTAB](../H/HTAB.md) (hash table type)
  - [HASHCTL](../H/HASHCTL.md) (hash table control structure)
  - [JoinHashEntry](../J/JoinHashEntry.md) (hash table entry structure)
  - [bitmap_hash](bitmap_hash.md) (hash function for Relids)
  - [bitmap_match](bitmap_match.md) (match function for Relids)
  - [hash_create](../h/hash_create.md) (creates the hash table)
  - [hash_search](../h/hash_search.md) (inserts entries into hash table)
  - HASH_ELEM, HASH_FUNCTION, HASH_COMPARE, HASH_CONTEXT (hash table flags)
  - HASH_ENTER (hash operation flag)
- Called from (representative examples):
  - [find_join_rel](../f/find_join_rel.md)

## Notes and Other Information
- This is a static function, only used internally within relnode.c
- Creates hash table with initial size of 256 entries
- Uses assertion to verify that no duplicate entries exist when populating the hash table
- The hash table enables O(1) lookup time for finding join relations by their Relids
- [Hash](../H/Hash.md) table is stored in CurrentMemoryContext
- Located in src/backend/optimizer/util/relnode.c:486-526