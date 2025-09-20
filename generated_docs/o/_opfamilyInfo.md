# _opfamilyInfo

## Location
[src/bin/pg_dump/pg_dump.h:275-279](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L275-L279)

## Overview
A structure definition used in PostgreSQL's pg_dump utility to represent operator family information for database dumping and restoration operations.

## Definition

```c
typedef struct _opfamilyInfo
{
	DumpableObject dobj;
	Oid			opfmethod;
	const char *rolname;
} OpfamilyInfo;
```
## Detailed Description
The  structure is part of PostgreSQL's pg_dump utility framework, designed to store metadata about operator families during database backup operations. Operator families in PostgreSQL are collections of related operator classes that support the same kinds of operations for an access method. They provide a higher-level organization structure above operator classes, allowing related operator classes to share operators and support functions. This structure extends the base  to include operator family-specific information, enabling pg_dump to properly serialize and restore operator family definitions.

## Parameters / Member Variables
- `dobj`: Base  structure containing common metadata for dumpable database objects (object ID, name, namespace, dump flags, etc.)
- `opfmethod`: OID (Object Identifier) of the access method this operator family is associated with (e.g., btree, hash, gist, gin, spgist, brin, etc.)
- `*rolname`: Pointer to constant string containing the name of the role (user) who owns this operator family
## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject (base structure)  
  - Oid (PostgreSQL object identifier type)
- Called from (representative examples):
  - [getOpfamilies](../g/getOpfamilies.md) (allocation and initialization of operator family arrays)
  - [dumpOpfamily](../d/dumpOpfamily.md) (for dumping operator family definitions)
  - Comparison functions in pg_dump_sort.c (for sorting operator families during dump)

## Notes and Other Information
- This structure is specifically used within the pg_dump utility context for backup and restore operations
- The structure is typedef'd as  for easier usage throughout the codebase
- Operator families are a higher-level abstraction above operator classes, introduced to allow sharing of operators between related operator classes
- Each operator family is tied to a specific access method through the  field
- Multiple operator classes can belong to the same operator family, allowing them to share operators and support functions
- The  field preserves ownership information critical for proper database restoration
- Operator families enable cross-data-type operations (e.g., comparing int2, int4, int8 within the same btree operator family)
- Part of PostgreSQL's extensible indexing system that supports custom data types and access methods