# CollInfo

## Location
[src/bin/pg_dump/pg_dump.h:287-288](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L287-L288)

## Overview
CollInfo represents collation objects in PostgreSQL's pg_dump utility, storing information about database collations that need to be dumped and restored.

## Definition

```c
typedef struct _convInfo
{
	DumpableObject dobj;
	const char *rolname;
} ConvInfo;
```
## Detailed Description
CollInfo is a structure used by pg_dump to encapsulate information about collation objects stored in the pg_collation system catalog. It extends the base DumpableObject structure to include collation-specific metadata required for dumping and restoring collations. The structure is populated by the getCollations() function during the schema discovery phase and later used by dumpCollation() to generate the appropriate CREATE COLLATION statements.

## Parameters / Member Variables
- : Base DumpableObject containing common dump metadata (object ID, name, namespace, dependencies, etc.)
- : Owner role name of the collation object, retrieved from pg_collation.collowner
- : Encoding associated with the collation, retrieved from pg_collation.collencoding

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject
  - [getRoleName](../g/getRoleName.md)
  - [findNamespace](../f/findNamespace.md)
  - [AssignDumpId](../A/AssignDumpId.md)
  - [selectDumpableObject](../s/selectDumpableObject.md)
- Called from (representative examples):
  - [getCollations](../g/getCollations.md) (src/bin/pg_dump/pg_dump.c:6106)
  - [dumpCollation](../d/dumpCollation.md) (src/bin/pg_dump/pg_dump.c:13842)
  - [findCollationByOid](../f/findCollationByOid.md) (src/bin/pg_dump/common.c:970)

## Notes and Other Information
- [CollInfo](CollInfo.md) objects are allocated as arrays in getCollations() function based on the number of collations found in pg_collation
- The structure inherits all functionality from DumpableObject including dependency tracking and selective dumping
- Collation encoding information is stored as an integer corresponding to PostgreSQL encoding IDs
- Used exclusively within the pg_dump utility for backup and restore operations