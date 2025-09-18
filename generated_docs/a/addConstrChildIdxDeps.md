# addConstrChildIdxDeps

## Location
src/bin/pg_dump/pg_dump.c: 7987 - 8009

## Overview
A recursive subroutine for getConstraints that establishes dependency relationships between foreign key constraints and partitioned index attachments to ensure proper restoration order during pg_dump operations.

## Definition


## Detailed Description
This function is a critical component of pg_dump's constraint handling system. It recursively traverses a partitioned index's partition attachments and marks a foreign key constraint object as dependent on each partition's DO_INDEX_ATTACH object. This dependency system ensures that during database restoration, foreign key constraints are not restored until all referenced indexes are fully validated and attached.

The function operates by walking through the partition attachment list of a referenced index and adding dependencies from the constraint object to each partition's index attachment object. If a partition itself has sub-partitions, the function recursively processes those as well, creating a complete dependency tree that mirrors the partitioning hierarchy.

## Parameters / Member Variables
- : A DumpableObject representing the foreign key constraint that needs dependencies established
- : A constant pointer to IndxInfo representing the partitioned index being referenced by the foreign key

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject (struct type)
  - IndxInfo (struct type)
  - SimplePtrListCell (struct type)
  - DO_FK_CONSTRAINT (enum value)
  - IndexAttachInfo (struct type)
  - addObjectDependency (function)
  - addConstrChildIdxDeps (recursive self-call)

- Called from (representative examples):
  - getConstraints (primary caller)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the pg_dump.c file
- The function includes an assertion to verify that the passed object is indeed a foreign key constraint (DO_FK_CONSTRAINT)
- The recursive nature handles arbitrarily deep partitioning hierarchies
- This dependency management is crucial for maintaining referential integrity during database restoration
- The function works specifically with partitioned indexes and their attachment objects, which are part of PostgreSQL's declarative partitioning feature