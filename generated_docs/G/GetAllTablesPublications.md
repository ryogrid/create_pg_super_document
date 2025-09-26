# GetAllTablesPublications

## Location
[src/backend/catalog/pg_publication.c:759-799](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_publication.c#L759-L799)

## Overview
Retrieves a list of publication OIDs for all publications that are marked as FOR ALL TABLES, used to identify publications that include all tables in the database.

## Definition
```c
List *GetAllTablesPublications(void)
```

## Detailed Description
This function scans the pg_publication system catalog to find all publications that have the `puballtables` flag set to true, indicating they are FOR ALL TABLES publications. These publications automatically include all user tables in the database, unlike FOR TABLE publications which explicitly list specific tables.

The function performs a sequential scan of the PublicationRelationId catalog table, filtering for publications where the puballtables column is true. It uses a boolean equality scan key to efficiently identify the matching publications. The function collects the OIDs of all matching publications and returns them as a list.

This function is typically used in replication contexts where the system needs to determine which publications apply to all tables, allowing for efficient bulk operations and determining publication membership without needing to check individual table-publication mappings.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [systable_endscan](../s/systable_endscan.md)
  - [table_close](../t/table_close.md)
  - [lappend_oid](../l/lappend_oid.md)
  - Form_pg_publication
  - [SysScanDesc](../S/SysScanDesc.md)
- Called from (representative examples):
  - [RelationBuildPublicationDesc](../R/RelationBuildPublicationDesc.md)

## Notes and Other Information
- Specifically targets publications with puballtables=true (FOR ALL TABLES publications)
- Uses sequential scan since it needs to check the puballtables boolean column
- Uses AccessShareLock for safe concurrent access to the catalog
- Returns NIL if no FOR ALL TABLES publications exist
- Properly manages catalog relation lifecycle with table_open/table_close
- This is complementary to GetPublicationRelations which handles FOR TABLE publications
- The returned list can be used to quickly determine if any publications include all tables without individual table lookups