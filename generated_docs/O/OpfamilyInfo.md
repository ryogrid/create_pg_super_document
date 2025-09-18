# OpfamilyInfo

## Location
[src/bin/pg_dump/pg_dump.h:280-281](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L280-L281)

## Overview
OpfamilyInfo is a structure used in pg_dump to represent operator family metadata during database dump operations, containing essential information about PostgreSQL operator families and their associated access methods.

## Definition


## Detailed Description
OpfamilyInfo stores metadata about PostgreSQL operator families for the dump and restore process. Operator families are collections of related operator classes that can be used together with an access method. They provide a way to group semantically compatible operator classes, allowing for more flexible and efficient query optimization. This structure captures the operator family's association with its access method and ownership information, enabling pg_dump to properly recreate custom operator families during database restoration.

## Parameters / Member Variables
- : DumpableObject containing basic dump metadata (OID, name, namespace, dependencies, etc.)
- : OID of the access method that this operator family belongs to (references pg_am.oid)
- : Name of the role/user who owns this operator family

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject (base structure for dump metadata)
  - Oid (PostgreSQL object identifier type)

- Called from (representative examples):
  - [getOpfamilies](../g/getOpfamilies.md) (src/bin/pg_dump/pg_dump.c:6395, 6420)
  - [dumpOpfamily](../d/dumpOpfamily.md) (src/bin/pg_dump/pg_dump.c:13623)
  - [dumpDumpableObject](../d/dumpDumpableObject.md) (src/bin/pg_dump/pg_dump.c:10565)
  - [DOTypeNameCompare](../D/DOTypeNameCompare.md) (src/bin/pg_dump/pg_dump_sort.c:313, 314)

## Notes and Other Information
- Operator families were introduced to provide a higher level of organization above operator classes
- The opfmethod field links the operator family to its specific access method (B-tree, Hash, GiST, etc.)
- Essential for maintaining custom indexing strategies and cross-type comparisons during database migration
- Works in conjunction with operator classes (OpclassInfo) to provide complete operator organization hierarchy
- Allows for more sophisticated query optimization by grouping compatible operator classes
- Located in src/bin/pg_dump/pg_dump.h:275-280