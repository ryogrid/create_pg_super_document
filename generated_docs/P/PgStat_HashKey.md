# PgStat_HashKey

## Location
[src/include/utils/pgstat_internal.h:52-57](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/pgstat_internal.h#L52-L57)

## Overview
PgStat_HashKey is the key structure used for the shared statistics hashtable that stores per-object statistics in PostgreSQL's statistics subsystem.

## Definition

```c
typedef struct PgStat_HashKey
{
	PgStat_Kind kind;			/* statistics entry kind */
	Oid			dboid;			/* database ID. InvalidOid for shared objects. */
	Oid			objoid;			/* object ID, either table or function. */
} PgStat_HashKey;
```
## Detailed Description
PgStat_HashKey serves as the unique identifier for entries in the shared statistics hashtable. This structure combines three key pieces of information to uniquely identify a statistics object: the kind of statistics being tracked, the database containing the object, and the specific object identifier. The shared hashtable (with entries of type PgStatShared_HashEntry) uses this key to efficiently locate and manage statistics data for various database objects like tables and functions.

The key design allows for hierarchical organization where statistics can be scoped by database (using dboid) and then by specific objects within that database (using objoid). For shared objects that exist across databases, InvalidOid is used for the dboid field.

## Parameters / Member Variables
- : Specifies the type of statistics entry (e.g., table stats, function stats) using PgStat_Kind enum
- : Database OID that contains the object; set to InvalidOid for shared objects that span databases
- : The specific object identifier, typically a table OID or function OID depending on the statistics kind

## Dependencies
- Functions called/Symbols referenced:
  - [PgStat_Kind](PgStat_Kind.md) (enum type)
- Called from (representative examples):
  - [pgstat_fetch_entry](../p/pgstat_fetch_entry.md) (statistics retrieval)
  - [pgstat_get_entry_ref](../p/pgstat_get_entry_ref.md) (reference management) 
  - [pgstat_drop_entry](../p/pgstat_drop_entry.md) (statistics cleanup)
  - [PgStatShared_HashEntry](PgStatShared_HashEntry.md) (as key field)
  - [pgstat_cmp_hash_key](../p/pgstat_cmp_hash_key.md) (key comparison function)
  - [pgstat_hash_hash_key](../p/pgstat_hash_hash_key.md) (key hashing function)

## Notes and Other Information
- This key structure is fundamental to PostgreSQL's shared statistics architecture, enabling efficient lookup and management of per-object statistics
- The combination of kind, dboid, and objoid must be unique across the entire shared statistics hashtable
- Used extensively in the statistics subsystem's hashtable operations for both shared memory access and local caching mechanisms
- The key supports both database-specific objects and shared objects through the dboid field design