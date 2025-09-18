# flagInhIndexes

## Location
src/bin/pg_dump/common.c: 411 - 500

## Overview
Creates IndexAttachInfo objects for partitioned indexes to handle index attachment operations during database restore.

## Definition


## Detailed Description
The flagInhIndexes function processes indexes on partitioned tables to create IndexAttachInfo objects that represent ATTACH INDEX operations needed during database restoration. When PostgreSQL creates indexes on partitioned tables, child partition indexes are automatically attached to their parent partitioned indexes. During pg_dump restore, these attachment relationships must be recreated after both the parent and child indexes exist.

The function iterates through all tables, focusing only on partition tables that have parent tables. For each index on a partition table that has a parent index, it creates an IndexAttachInfo object that will generate the appropriate ATTACH INDEX command during restore. The function establishes explicit dependencies to ensure that both the parent and child indexes, as well as their underlying tables, exist before the attachment operation is attempted.

## Parameters / Member Variables
- : Archive structure containing database connection and dump configuration
- : Array of TableInfo structures representing all tables in the database  
- : Number of tables in the tblinfo array

## Dependencies
- Functions called/Symbols referenced:
  - [findIndexByOid](findIndexByOid.md) (locates parent index by OID)
  - [AssignDumpId](../A/AssignDumpId.md) (assigns unique dump IDs to IndexAttachInfo objects)
  - [addObjectDependency](../a/addObjectDependency.md) (establishes dump order dependencies)
  - [simple_ptr_list_append](../s/simple_ptr_list_append.md) (adds to parent index's partition list)
  - pg_malloc_object (memory allocation)
- Called from (representative examples):
  - [getSchemaData](../g/getSchemaData.md) (src/bin/pg_dump/common.c:242)

## Notes and Other Information
The function only processes partition tables (not regular inheritance), as indicated by the ispartition check. Each partition table can have only one parent, which is verified by an assertion. The dependencies established include not only the indexes themselves but also their underlying tables to prevent parallel restore operations from interfering with each other.

The IndexAttachInfo objects created have the DO_INDEX_ATTACH object type and are tracked in the parent index's partattaches list for organizational purposes during the dump process. This ensures that index attachment operations occur in the proper sequence during database restoration.