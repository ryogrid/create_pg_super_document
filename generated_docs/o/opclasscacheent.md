# opclasscacheent

## Location
[src/backend/utils/cache/relcache.c:261-268](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L261-L268)

## Overview
The  structure provides a specialized cache for operator class (opclass) related information, storing essential metadata about operator classes and their associated support procedures.

## Definition

```c
typedef struct opclasscacheent
{
	Oid			opclassoid;		/* lookup key: OID of opclass */
	bool		valid;			/* set true after successful fill-in */
	StrategyNumber numSupport;	/* max # of support procs (from pg_am) */
	Oid			opcfamily;		/* OID of opclass's family */
	Oid			opcintype;		/* OID of opclass's declared input type */
	RegProcedure *supportProcs; /* OIDs of support procedures */
} OpClassCacheEnt;
```
## Detailed Description
The  structure serves as a cache entry in the operator class cache () within PostgreSQL's relation cache system. This cache is specifically designed to store opclass-related information to avoid repeated lookups in the system catalogs. The structure caches essential metadata about operator classes, including their family membership, input types, and associated support procedures. An important limitation is that only default support procedures are cached - specifically those where . This design decision optimizes for the most common case while keeping the cache structure manageable. The cache improves performance for operations that need to access operator class information, such as index operations and operator resolution.

## Parameters / Member Variables
- : The object identifier (OID) of the operator class, serving as the unique lookup key for hash table operations
- : A boolean flag that indicates whether the cache entry has been successfully populated with valid data
- : The maximum number of support procedures for this operator class, obtained from the corresponding access method (pg_am)
- : The OID of the operator family to which this operator class belongs
- : The OID of the data type that this operator class is declared to handle as input
- : A pointer to an array of  values containing the OIDs of the support procedures associated with this operator class

## Dependencies
- Functions called/Symbols referenced:
  - StrategyNumber (type for numbering strategy procedures)
  - RegProcedure (type for procedure OIDs)
- Called from (representative examples):
  - Used internally by OpClassCache hash table operations
  - Referenced by operator class lookup and caching functions

## Notes and Other Information
- This structure is part of the internal caching system and is not exposed to external modules
- Only default support procedures are cached (where lefttype = righttype = opcintype), which covers the most common usage patterns
- The cache is managed by the static  hash table variable
- Essential for performance optimization in index operations and operator resolution
- Defined in  starting at line 261
- The  flag follows the typical PostgreSQL pattern of lazy initialization for cache entries
- Support procedures are fundamental to how PostgreSQL implements various index access methods and operator behaviors