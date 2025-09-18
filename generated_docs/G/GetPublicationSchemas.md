# GetPublicationSchemas

## Location
src/backend/catalog/pg_publication.c: 861 - 898

## Overview
Retrieves a list of schema OIDs associated with a specific FOR TABLES IN SCHEMA publication, used to determine which schemas are included in schema-based publications.

## Definition
```c
List *GetPublicationSchemas(Oid pubid)
```

## Detailed Description
This function queries the pg_publication_namespace system catalog to find all schemas that belong to a specific publication configured as FOR TABLES IN SCHEMA. It performs an indexed scan of the PublicationNamespaceRelationId catalog table, filtering by the publication OID to retrieve all associated schema OIDs.

The function is specifically designed for schema-based publications and should only be used with FOR TABLES IN SCHEMA publications. It uses the PublicationNamespacePnnspidPnpubidIndexId index for efficient lookup based on the publication ID, ensuring optimal performance when retrieving schema associations.

Each tuple found in the scan represents a publication-namespace relationship, from which the function extracts the namespace (schema) OID and adds it to the result list. The function properly manages catalog resources by opening and closing the relation with appropriate locking.

## Parameters / Member Variables
- `pubid`: The OID of the publication for which to retrieve associated schemas

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [systable_endscan](../s/systable_endscan.md)
  - table_close
  - lappend_oid
  - Form_pg_publication_namespace
  - [SysScanDesc](../S/SysScanDesc.md)
- Called from (representative examples):
  - [GetAllSchemaPublicationRelations](GetAllSchemaPublicationRelations.md)
  - [AlterPublicationSchemas](../A/AlterPublicationSchemas.md)

## Notes and Other Information
- Specifically for FOR TABLES IN SCHEMA publications only
- Uses indexed scan with PublicationNamespacePnnspidPnpubidIndexId for efficient catalog lookup
- Returns NIL if no schemas are associated with the publication
- Uses AccessShareLock for safe concurrent access to the catalog
- Properly manages catalog relation lifecycle with table_open/table_close
- The returned schema OIDs can be used to determine which tables are implicitly included through schema membership
- Essential for schema-based publication operations and determining publication scope in replication contexts