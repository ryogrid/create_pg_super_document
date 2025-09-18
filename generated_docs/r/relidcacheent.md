# relidcacheent

## Location
[src/backend/utils/cache/relcache.c:128-131](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L128-L131)

## Overview
The  is a hash table entry structure used to index the PostgreSQL relation cache by relation OID, providing fast lookup of cached relation descriptors.

## Definition


## Detailed Description
The  structure serves as an entry in the relation cache hash table (). It implements a simple key-value mapping where the relation OID () acts as the key and the relation descriptor () serves as the cached value. This structure is part of PostgreSQL's relation cache system, which maintains frequently accessed relation metadata in memory to avoid repeated catalog lookups. Historically, PostgreSQL indexed the relation cache by both name and OID, but the current implementation only maintains an index by OID for performance reasons.

## Parameters / Member Variables
- : The object identifier (OID) of the relation, serving as the unique key for hash table lookups
- : A pointer to the cached  structure containing the complete relation descriptor with metadata such as tuple descriptor, access methods, and other relation properties

## Dependencies
- Functions called/Symbols referenced:
  - Oid (built-in type)
  - [Relation](../R/Relation.md) (type from relation cache system)
- Called from (representative examples):
  - Used internally by RelationIdCache hash table operations
  - Referenced by relation cache management functions

## Notes and Other Information
- This structure is part of the internal implementation of the relation cache and is not exposed to external modules
- The relation cache is critical for PostgreSQL performance as it avoids expensive catalog lookups for frequently accessed relations
- The structure is defined in  starting at line 128
- The hash table using this structure () is a static variable managing the global relation cache