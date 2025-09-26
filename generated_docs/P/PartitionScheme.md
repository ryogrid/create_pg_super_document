# PartitionScheme

## Location
[src/include/nodes/pathnodes.h:598-816](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L598-L816)

## Overview
PartitionScheme is a pointer type (typedef) that references PartitionSchemeData structures, providing a handle to shared partition scheme information across multiple partitioned relations.

## Definition
```c
typedef struct PartitionSchemeData *PartitionScheme;
```

## Detailed Description
PartitionScheme serves as a pointer type that allows multiple partitioned relations with identical partitioning schemes to share the same underlying PartitionSchemeData structure. This design promotes memory efficiency and consistency across the PostgreSQL query planner when dealing with partitioned tables that use the same partitioning strategy and attributes.

The typedef abstraction provides a clean interface for passing partition scheme information throughout the planner without exposing the internal structure details. This is particularly important in the context of partition-wise joins and partition pruning operations where the planner needs to compare and manipulate partition schemes across different relations.

## Parameters / Member Variables
- This is a typedef pointer, so it inherits all members from PartitionSchemeData:
  - Access to strategy, partnatts, partopfamily, partopcintype, partcollation
  - Access to cached type information (parttyplen, parttypbyval)
  - Access to cached function information (partsupfunc)

## Dependencies
- Functions called/Symbols referenced:
  - PartitionSchemeData (the underlying structure at line 598)
- Called from (representative examples):
  - compute_partition_bounds (src/backend/optimizer/path/joinrels.c:1801)
  - partkey_is_bool_constant_for_query (src/backend/optimizer/path/pathkeys.c:844)
  - build_partition_pathkeys (src/backend/optimizer/path/pathkeys.c:921)
  - set_relation_partition_info (src/backend/optimizer/util/plancat.c:2448)
  - find_partition_scheme (src/backend/optimizer/util/plancat.c:2455)
  - build_joinrel_partition_info (src/backend/optimizer/util/relnode.c:2022)
  - have_partkey_equi_join (src/backend/optimizer/util/relnode.c:2094)
  - gen_partprune_steps_internal (src/backend/partitioning/partprune.c:964)
  - RelOptInfo (src/include/nodes/pathnodes.h:1009)

## Notes and Other Information
- Used extensively throughout the PostgreSQL optimizer for partition-related operations
- Enables efficient sharing of partition scheme metadata across multiple relations
- Critical component in partition-wise join optimization and partition pruning
- The pointer abstraction allows for easy comparison of partition schemes between relations
- Part of the broader partitioning infrastructure that supports table inheritance and declarative partitioning