# JoinHashEntry

## Location
[src/backend/optimizer/util/relnode.c:38-42](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/relnode.c#L38-L42)

## Overview
JoinHashEntry is a hash table entry structure used by PostgreSQL's query planner to efficiently store and lookup join relations during query optimization.

## Definition


## Detailed Description
JoinHashEntry serves as an auxiliary data structure that enables fast lookups of join relations in the query planner's optimization process. It acts as an entry in a hash table where each entry maps a set of relation identifiers (join_relids) to the corresponding RelOptInfo structure that represents the join relation.

The structure is specifically designed for hash table operations, with the join_relids field positioned first to serve as the hash key. This design allows the planner to quickly determine whether a join relation for a specific set of base relations has already been created, avoiding redundant work during join enumeration.

The hash table using JoinHashEntry is built on-demand when the planner needs to perform frequent lookups of existing join relations, particularly during the dynamic programming approach to join ordering optimization.

## Parameters / Member Variables
- : A Relids (Bitmapset*) representing the set of base relation identifiers that participate in this join. This field serves as the hash key and must be positioned first in the structure for hash table operations.
- : A pointer to the RelOptInfo structure that contains detailed information about the join relation, including cost estimates, access paths, and other optimization data.

## Dependencies
- Functions called/Symbols referenced:
  - Relids (typedef for Bitmapset*)
  - RelOptInfo
- Called from (representative examples):
  - [build_join_rel_hash](../b/build_join_rel_hash.md) (creates hash entries for existing join relations)
  - [find_join_rel](../f/find_join_rel.md) (searches for existing join relations using hash lookup)
  - [add_join_rel](../a/add_join_rel.md) (adds new join relations to the hash table)

## Notes and Other Information
- The join_relids field is marked with a comment indicating it must be first, which is a requirement for the hash table implementation used by PostgreSQL
- This structure is part of the query planner's internal data structures and is not exposed to external interfaces
- The hash table containing these entries is created using PostgreSQL's internal HTAB interface with custom hash and comparison functions for Relids
- Located in src/backend/optimizer/util/relnode.c:38-42
- Used exclusively within the query optimization subsystem to improve performance of join relation lookups during planning