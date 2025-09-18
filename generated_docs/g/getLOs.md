# getLOs

## Location
src/bin/pg_dump/pg_dump.c: 3676 - 3813

## Overview
The  function collects schema-level metadata about large objects (BLOBs) from the database and creates DumpableObject structures for efficient dumping and restoration.

## Definition


## Detailed Description
The  function queries the pg_largeobject_metadata table to retrieve information about all large objects in the database, including their OIDs, owners, and ACL settings. It groups large objects with identical ownership and ACL settings into batches (up to MAX_BLOBS_PER_ARCHIVE_ENTRY per group) for efficient processing. For each group, it creates both a metadata DumpableObject (LoInfo) containing ownership and permission information, and a separate data DumpableObject for the actual BLOB content. This design allows for proper dependency tracking and selective dumping. The function handles special cases like binary upgrade mode where BLOB data is excluded since pg_upgrade handles it separately.

## Parameters / Member Variables
- : Pointer to the Archive structure representing the output dump file and containing dump options

## Dependencies
- Functions called/Symbols referenced:
  - ExecuteSqlQuery (executes the LO metadata query)
  - createPQExpBuffer/destroyPQExpBuffer (query string management)
  - pg_log_info (logs the operation)
  - atooid (converts string OID to Oid type)
  - pg_malloc/pg_strdup (memory allocation and string duplication)
  - AssignDumpId (assigns unique dump IDs to objects)
  - getRoleName (resolves owner name from OID)
  - recordAdditionalCatalogID (enables lookup by secondary OIDs)
  - DumpOptions/LoInfo/DumpableObject/CatalogId (data structures)
  - DO_LARGE_OBJECT/DO_LARGE_OBJECT_DATA (object type constants)
  - DUMP_COMPONENT_DATA/DUMP_COMPONENT_ACL (component flags)
- Called from (representative examples):
  - main (pg_dump main function)
  - fmtQualifiedDumpable

## Notes and Other Information
- Groups BLOBs by owner and ACL to reduce the number of archive entries and improve efficiency
- Creates separate metadata and data objects to enable proper dependency relationships
- In binary upgrade mode, excludes BLOB data since pg_upgrade copies pg_largeobject table directly
- Uses recordAdditionalCatalogID to allow lookup of LoInfo by any BLOB OID in the group
- Handles both single BLOBs and BLOB ranges in naming (e.g., '12345' vs '12345..12350')
- Essential for proper BLOB backup and restoration in PostgreSQL dumps