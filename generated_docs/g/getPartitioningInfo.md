# getPartitioningInfo

## Location
[src/bin/pg_dump/pg_dump.c:7373-7432](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L7373-L7432)

## Overview
Identifies partitioned tables with "unsafe" partitioning schemes that require load-via-partition-root mode during pg_dump operations, specifically focusing on hash partitioning on enum columns.

## Definition


## Detailed Description
The getPartitioningInfo function analyzes all partitioned tables in the database to identify those with partitioning schemes that are considered "unsafe" for normal dump and restore operations. Currently, the primary concern is hash partitioning on enum columns, where hash codes depend on enum value OIDs that won't be preserved across dump-and-reload cycles. The function queries pg_partitioned_table and related catalogs to find tables using hash partitioning with enum_ops operator classes, then marks these tables as requiring special handling during data loading. This ensures data integrity during backup and restore operations by forcing the use of partition root tables for data insertion.

## Parameters / Member Variables
- : Archive pointer containing dump configuration and remote version information

## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - PGRES_TUPLES_OK (constant)
  - atooid
  - [TableInfo](../T/TableInfo.md) (structure type)
  - [findTableByOid](../f/findTableByOid.md)
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md)
  - [SubRelInfo](../S/SubRelInfo.md) (referenced in header)

## Notes and Other Information
- Only processes databases with PostgreSQL version 11.0000 or higher (hash partitioning introduction)
- Skips processing entirely for schema-only dumps since data loading isn't involved
- The function examines all partitioned tables, not just those being dumped, to handle parent-child relationships correctly
- Sets the unsafe_partitions flag on affected TableInfo structures to influence later dump behavior
- The safety check is specifically for hash partitioning with enum_ops operator class in the pg_catalog namespace
- Handles tables that may not have locks since it only queries catalog information without invoking server-side functions