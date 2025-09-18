# flagInhTables

## Location
src/bin/pg_dump/common.c: 293 - 410

## Overview
Establishes parent-child relationships for inheritance hierarchies and marks parent tables as interesting for dump processing.

## Definition


## Detailed Description
The flagInhTables function processes PostgreSQL table inheritance information to establish proper parent-child relationships within the TableInfo structures. It serves two primary purposes: first, it creates bidirectional links between child tables and their parent tables by populating the parents array in each child TableInfo; second, it marks parent tables of dumpable tables as 'interesting' so they will be processed during subsequent phases like getTableAttrs and getIndexes.

The function also handles partition table attachments by creating TableAttachInfo objects for partitioned tables. These objects represent the ATTACH PARTITION operations needed to properly recreate the partitioning structure during database restoration. The function ensures proper dependency ordering by making TableAttachInfo objects depend on both the partition table and its parent table.

## Parameters / Member Variables
- : Archive structure containing database connection and dump configuration
- : Array of TableInfo structures representing all tables in the database
- : Number of tables in the tblinfo array
- : Array of InhInfo structures containing inheritance relationship data from pg_inherits
- : Number of inheritance relationships in the inhinfo array

## Dependencies
- Functions called/Symbols referenced:
  - findTableByOid (locates TableInfo by OID)
  - AssignDumpId (assigns unique dump IDs to objects)
  - addObjectDependency (establishes dump order dependencies)
  - pg_realloc_array, pg_malloc_array (memory management)
- Called from (representative examples):
  - getSchemaData (src/bin/pg_dump/common.c:227)

## Notes and Other Information
The function includes performance optimizations by caching the last-used child and parent TableInfo pointers to avoid repeated hash table lookups when processing consecutive inheritance records for the same tables. Only direct ancestors of target tables are marked as interesting, which is sufficient for pg_dump's needs since inherited attributes don't require special handling beyond ensuring the parent structure exists.

For partitioned tables, the function creates TableAttachInfo objects that will generate ATTACH PARTITION commands during the dump restoration process. These objects have explicit dependencies on both the parent and child tables to ensure proper creation order during restoration.