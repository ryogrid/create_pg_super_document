# TableAttachInfo

## Location
[src/bin/pg_dump/pg_dump.h:386-387](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L386-L387)

## Overview
TableAttachInfo represents partition attachment operations in PostgreSQL's pg_dump utility, storing the relationship between a partitioned table and its partitions for proper restoration ordering.

## Definition


## Detailed Description
TableAttachInfo is a specialized structure used by pg_dump to handle table partitioning relationships. It represents the ATTACH PARTITION operation that must be executed after both the partitioned parent table and the partition table are created during database restoration. This structure is created during the schema discovery phase for each partition found in the database, ensuring that partition attachments are performed in the correct order during restoration. The structure exists solely to manage dependencies and generate ALTER TABLE ... ATTACH PARTITION statements.

## Parameters / Member Variables
- : Base DumpableObject containing common dump metadata (object ID, name, namespace, dependencies, etc.)
- : Pointer to the TableInfo structure representing the partitioned parent table
- : Pointer to the TableInfo structure representing the partition table to be attached

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject
  - [TableInfo](TableInfo.md)
  - [AssignDumpId](../A/AssignDumpId.md)
  - [addObjectDependency](../a/addObjectDependency.md)
- Called from (representative examples):
  - [flagInhTables](../f/flagInhTables.md) (src/bin/pg_dump/common.c:372)
  - [dumpTableAttach](../d/dumpTableAttach.md) (src/bin/pg_dump/pg_dump.c:16808)
  - fmtQualifiedDumpable (src/bin/pg_dump/pg_dump.c:256)

## Notes and Other Information
- [TableAttachInfo](TableAttachInfo.md) objects are created automatically during flagInhTables() for tables identified as partitions
- Each partition can have only one parent table, which is enforced during TableAttachInfo creation
- The structure establishes explicit dependencies on both parent and partition tables to ensure correct restoration order
- Dependencies are manually added since partition attachment relationships are not stored in pg_depend
- The dumpTableAttach() function uses this information to generate ALTER TABLE ... ATTACH PARTITION statements
- Object name matches the partition table name for identification purposes
- Used exclusively for declarative partitioning (not inheritance-based partitioning)
- Essential for maintaining partition hierarchy during database restoration