# ForeignKeyCacheInfo

## Location
src/include/utils/rel.h: 273 - 296

## Overview
ForeignKeyCacheInfo is a structure that caches foreign key constraint information in the relation cache, providing efficient access to constraint metadata without repeated catalog lookups.

## Definition


## Detailed Description
ForeignKeyCacheInfo serves as a cached representation of foreign key constraint information derived from the pg_constraint system catalog. This structure is designed to provide efficient access to foreign key metadata that is frequently needed by the query planner and other system components, without requiring repeated expensive catalog lookups.

The structure is implemented as a Node subclass, making it compatible with PostgreSQL's standard node copying and manipulation functions. However, it's designed as a "flat" structure without substructure, which allows efficient memory management using simple list operations.

The structure caches the essential information about a foreign key constraint, including the OIDs of both the referencing and referenced relations, the column mappings between them, and the equality operators used for constraint checking. This information is particularly valuable to the query planner for optimization decisions involving foreign key relationships.

## Parameters / Member Variables
- : NodeTag identifier for the node type system
- : Object identifier of the foreign key constraint itself in pg_constraint
- : OID of the relation that contains the foreign key (referencing table)
- : OID of the relation that is referenced by the foreign key (referenced table)
- : Number of columns participating in the foreign key constraint
- : Array of attribute numbers for columns in the referencing table (size limited by INDEX_MAX_KEYS)
- : Array of attribute numbers for columns in the referenced table (size limited by INDEX_MAX_KEYS)
- : Array of OIDs for the equality operators used to compare primary key and foreign key values (size limited by INDEX_MAX_KEYS)

## Dependencies
- Functions called/Symbols referenced:
  - INDEX_MAX_KEYS (constant defining maximum array size)
  - NodeTag, Oid, AttrNumber (basic PostgreSQL types)
- Called from (representative examples):
  - RelationGetFKeyList (relcache management)
  - get_relation_foreign_keys (planner interface)
  - addFkRecurseReferencing (DDL operations)
  - CloneFkReferencing (partition management)
  - DetachPartitionFinalize (partition operations)

## Notes and Other Information
- The structure is primarily used by the query planner to make optimization decisions based on foreign key relationships
- Arrays are fixed-size with INDEX_MAX_KEYS elements, but only the first nkeys entries contain valid data
- The "flat" design without substructure allows efficient memory management and list operations
- Node attributes (no_equal, no_read, no_query_jumble) indicate this structure should not participate in certain node operations
- This caching mechanism significantly improves performance for queries involving tables with many foreign key relationships
- The structure has grown over time as new use cases have been identified, originally focused on planner needs but now including constraint OIDs for other purposes