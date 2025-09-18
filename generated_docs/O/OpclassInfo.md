# OpclassInfo

## Location
src/bin/pg_dump/pg_dump.h: 273 - 274

## Overview
OpclassInfo is a structure used in pg_dump to represent operator class metadata during database dump operations, containing essential information about PostgreSQL operator classes and their associated access methods.

## Definition


## Detailed Description
OpclassInfo stores metadata about PostgreSQL operator classes for the dump and restore process. Operator classes define sets of operators and support functions that an access method can use for a particular data type. This structure captures the operator class's association with its access method and ownership information, enabling pg_dump to properly recreate custom operator classes during database restoration with their correct relationships and permissions.

## Parameters / Member Variables
- : DumpableObject containing basic dump metadata (OID, name, namespace, dependencies, etc.)
- : OID of the access method that this operator class belongs to (references pg_am.oid)
- : Name of the role/user who owns this operator class

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject (base structure for dump metadata)
  - Oid (PostgreSQL object identifier type)

- Called from (representative examples):
  - [getOpclasses](../g/getOpclasses.md) (src/bin/pg_dump/pg_dump.c:6326, 6349)
  - [dumpOpclass](../d/dumpOpclass.md) (src/bin/pg_dump/pg_dump.c:13342)
  - [dumpDumpableObject](../d/dumpDumpableObject.md) (src/bin/pg_dump/pg_dump.c:10562)
  - [DOTypeNameCompare](../D/DOTypeNameCompare.md) (src/bin/pg_dump/pg_dump_sort.c:302, 303)

## Notes and Other Information
- Operator classes are fundamental to PostgreSQL's indexing system, defining how data types can be indexed and searched
- The opcmethod field links the operator class to its specific access method (B-tree, Hash, GiST, etc.)
- Essential for maintaining custom indexing strategies and data type support during database migration
- Works in conjunction with operator families (OpfamilyInfo) to provide complete operator organization
- Located in src/bin/pg_dump/pg_dump.h:268-273