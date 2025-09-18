# GetAllTablesPublicationRelations

## Location
src/backend/catalog/pg_publication.c: 800 - 860

## Overview
Retrieves a list of all relation OIDs that would be published by FOR ALL TABLES publications, with intelligent handling of partitioned tables based on publication settings.

## Definition
```c
List *GetAllTablesPublicationRelations(bool pubviaroot)
```

## Detailed Description
This function scans the pg_class system catalog to identify all relations that would be included in FOR ALL TABLES publications. It handles the complex logic around partitioned tables based on the `pubviaroot` parameter, which determines whether publications should include partitions individually or publish partition changes via their root partitioned tables.

When `pubviaroot` is false, the function includes all publishable regular tables (RELKIND_RELATION) that are not partitions. When `pubviaroot` is true, it excludes individual partitions and instead includes the root partitioned tables (RELKIND_PARTITIONED_TABLE), ensuring that partition changes are published through their parent tables rather than individually.

The function performs two separate scans when `pubviaroot` is true: first for regular tables (excluding partitions), then for partitioned tables (excluding those that are themselves partitions of other tables). This approach ensures proper handling of nested partitioning scenarios.

## Parameters / Member Variables
- `pubviaroot`: Boolean flag indicating whether to publish partitions via their root partitioned tables (true) or include partitions individually (false)

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - ScanKeyInit
  - table_beginscan_catalog
  - heap_getnext
  - table_endscan
  - table_close
  - is_publishable_class
  - lappend_oid
  - Form_pg_class
  - TableScanDesc
  - ForwardScanDirection
  - RELKIND_RELATION
  - RELKIND_PARTITIONED_TABLE
  - CharGetDatum
- Called from (representative examples):
  - NUM_PUBLICATION_TABLES_ELEM

## Notes and Other Information
- Performs catalog scans on pg_class to identify publishable relations
- Uses is_publishable_class to determine if a relation should be included in publications
- Handles complex partitioning logic: when pubviaroot=true, excludes individual partitions in favor of root partitioned tables
- May perform two separate scans depending on pubviaroot parameter
- Uses table_beginscan_catalog for efficient catalog scanning
- Properly manages AccessShareLock for safe concurrent catalog access
- The pubviaroot parameter corresponds to the publish_via_partition_root publication option
- Essential for determining the complete set of tables affected by FOR ALL TABLES publications