# table_beginscan_catalog

## Location
[src/backend/access/table/tableam.c:113-130](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/table/tableam.c#L113-L130)

## Overview
Initiates a sequential scan on a catalog (system) table using a catalog-appropriate snapshot with specific scan options optimized for system catalog access.

## Definition

```c
TableScanDesc
table_beginscan_catalog(Relation relation, int nkeys, struct ScanKeyData *key)
```
## Detailed Description
This function is specifically designed for scanning PostgreSQL system catalog tables. It sets up a table scan with parameters and options that are appropriate for catalog access:

1. **Scan Configuration**: Uses a combination of scan options (SO_TYPE_SEQSCAN, SO_ALLOW_STRAT, SO_ALLOW_SYNC, SO_ALLOW_PAGEMODE, SO_TEMP_SNAPSHOT) optimized for catalog scanning
2. **Snapshot Management**: Obtains a catalog snapshot using GetCatalogSnapshot() and registers it for proper resource management
3. **Access Method Delegation**: Delegates the actual scan initiation to the relation's table access method

The function is part of the table access method abstraction layer but provides catalog-specific behavior, ensuring that system catalog scans use appropriate snapshots that are sufficiently up-to-date for catalog consistency.

## Parameters / Member Variables
- : The catalog relation to scan
- : Number of scan keys (filters) to apply during the scan
- : Array of ScanKeyData structures defining the scan conditions/filters

## Dependencies
- Functions called/Symbols referenced:
  - [GetCatalogSnapshot](../G/GetCatalogSnapshot.md) (to obtain appropriate snapshot for catalog access)
  - RegisterSnapshot (to register snapshot for resource management)
  - RelationGetRelid (macro to get relation OID)
  - SO_TYPE_SEQSCAN, SO_ALLOW_STRAT, SO_ALLOW_SYNC, SO_ALLOW_PAGEMODE, SO_TEMP_SNAPSHOT (scan option flags)

- Called from (representative examples):
  - [objectsInSchemaToOids](../o/objectsInSchemaToOids.md)
  - [getRelationsInNamespace](../g/getRelationsInNamespace.md)
  - [GetAllTablesPublicationRelations](../G/GetAllTablesPublicationRelations.md)
  - [ReindexMultipleTables](../R/ReindexMultipleTables.md)
  - [get_all_vacuum_rels](../g/get_all_vacuum_rels.md)
  - [do_autovacuum](../d/do_autovacuum.md)
  - [get_database_list](../g/get_database_list.md)
  - get_subscription_list

## Notes and Other Information
- This function is specifically for system catalog tables and should not be used for regular user tables
- The catalog snapshot ensures that the scan sees a consistent view of the system catalogs, which is critical for system metadata operations
- The scan options combination allows for various optimizations while maintaining the temporary snapshot semantics
- The function is heavily used by system maintenance operations like autovacuum, reindexing, and schema management
- Unlike regular table scans, catalog scans often need to see recent changes to system metadata, which is handled by the catalog snapshot mechanism
- The registered snapshot ensures proper cleanup when the scan is completed