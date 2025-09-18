# GetSchemaPublications

## Location
[src/backend/catalog/pg_publication.c:899-924](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_publication.c#L899-L924)

## Overview
Retrieves a list of publication OIDs that are associated with a specified schema, used for logical replication to determine which publications include tables from a particular schema.

## Definition
List *GetSchemaPublications(Oid schemaid)

## Detailed Description
This function searches the PostgreSQL system catalog to find all publications that include the specified schema. It uses the system cache to efficiently look up publication-namespace mappings and returns a list of publication OIDs. This is essential for logical replication functionality where publications can be defined to include entire schemas rather than individual tables.

The function performs a systematic search through the PUBLICATIONNAMESPACEMAP system cache, which maintains the relationship between publications and the schemas they include. For each matching entry, it extracts the publication OID and builds a result list.

## Parameters / Member Variables
- : The OID of the schema for which to find associated publications

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCacheList1: Searches system cache for publication-namespace mappings
  - lappend_oid: Appends an OID to the result list
  - ReleaseSysCacheList: Releases the system cache list to free memory
  - GETSTRUCT: Macro to extract structure from heap tuple
- Called from (representative examples):
  - [GetTopMostAncestorInPublication](GetTopMostAncestorInPublication.md): Checks publication membership for table inheritance
  - [get_rel_sync_entry](../g/get_rel_sync_entry.md): Determines replication synchronization requirements
  - [RelationBuildPublicationDesc](../R/RelationBuildPublicationDesc.md): Builds publication descriptions for relation cache

## Notes and Other Information
- Returns NIL (empty list) if no publications are associated with the specified schema
- The function handles memory management by releasing the system cache list after processing
- Part of PostgreSQL's logical replication infrastructure for schema-level publication support
- The result list contains publication OIDs that can be used for further publication-related operations