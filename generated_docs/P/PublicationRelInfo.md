# PublicationRelInfo

## Location
[src/include/catalog/pg_publication.h:109-114](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/catalog/pg_publication.h#L109-L114)

## Overview
PublicationRelInfo is a structure used by pg_dump to represent the relationship between a publication and a specific table, including associated metadata like row filters and column lists.

## Definition


## Detailed Description
PublicationRelInfo is a pg_dump-specific structure that represents the association between a publication and an individual table. This structure is part of pg_dump's internal data model for capturing and reconstructing logical replication publication configurations during database backup and restore operations.

The structure extends the base DumpableObject to provide standard dump/restore functionality while adding publication-specific metadata. It captures not only the basic publication-table relationship but also advanced features like row filters (WHERE clauses) and column lists that may be associated with the publication of a specific table.

This structure is essential for pg_dump to accurately recreate publication configurations, ensuring that complex replication setups with row filters and column-level publication can be properly backed up and restored.

## Parameters / Member Variables
- : DumpableObject base structure providing standard dump/restore functionality and object metadata
- : Pointer to PublicationInfo structure representing the publication that includes this table
- : Pointer to TableInfo structure representing the table being published
- : String containing the row filter (WHERE clause) applied to this table in the publication, or NULL if no filter
- : String containing the column list specification for this table in the publication, or NULL if all columns are published

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject (inherited base structure)
  - PublicationInfo (referenced via publication pointer)
  - [TableInfo](../T/TableInfo.md) (referenced via pubtable pointer)
- Called from (representative examples):
  - [getPublicationTables](../g/getPublicationTables.md) (src/bin/pg_dump/pg_dump.c:4526)
  - [dumpPublicationTable](../d/dumpPublicationTable.md) (src/bin/pg_dump/pg_dump.c:4697)
  - [dumpDumpableObject](../d/dumpDumpableObject.md) (src/bin/pg_dump/pg_dump.c:10691)
  - [DOTypeNameCompare](../D/DOTypeNameCompare.md) (src/bin/pg_dump/pg_dump_sort.c:435)

## Notes and Other Information
- This structure is specific to pg_dump and is not used in the core PostgreSQL server code
- The structure is designed to capture the full complexity of publication-table relationships including advanced features
- Row filters (pubrelqual) and column lists (pubrattrs) are stored as text strings for serialization in dump files
- The structure inherits standard dump/restore functionality from DumpableObject
- Memory management for the string fields (pubrelqual, pubrattrs) follows pg_dump's allocation patterns
- This is one of several similar structures (like PublicationSchemaInfo) that handle different aspects of publication dumping