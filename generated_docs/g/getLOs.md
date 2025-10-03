# getLOs

## Location
[src/bin/pg_dump/pg_dump.c:3676-3813](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L3676-L3813)

## Overview
The  function collects schema-level metadata about large objects (BLOBs) from the database and creates DumpableObject structures for efficient dumping and restoration.

## Definition

```c
static void
getLOs(Archive *fout)
```
## Detailed Description
The  function queries the pg_largeobject_metadata table to retrieve information about all large objects in the database, including their OIDs, owners, and ACL settings. It groups large objects with identical ownership and ACL settings into batches (up to MAX_BLOBS_PER_ARCHIVE_ENTRY per group) for efficient processing. For each group, it creates both a metadata DumpableObject (LoInfo) containing ownership and permission information, and a separate data DumpableObject for the actual BLOB content. This design allows for proper dependency tracking and selective dumping. The function handles special cases like binary upgrade mode where BLOB data is excluded since pg_upgrade handles it separately.

## Parameters / Member Variables
- `*fout`: Pointer to the Archive structure representing the output dump file and containing dump options
## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md) (executes the LO metadata query)
  - [createPQExpBuffer](../c/createPQExpBuffer.md)/destroyPQExpBuffer (query string management)
  - pg_log_info (logs the operation)
  - atooid (converts string OID to Oid type)
  - [pg_malloc](../p/pg_malloc.md)/pg_strdup (memory allocation and string duplication)
  - [AssignDumpId](../A/AssignDumpId.md) (assigns unique dump IDs to objects)
  - [getRoleName](getRoleName.md) (resolves owner name from OID)
  - [recordAdditionalCatalogID](../r/recordAdditionalCatalogID.md) (enables lookup by secondary OIDs)
  - DumpOptions/LoInfo/DumpableObject/CatalogId (data structures)
  - DO_LARGE_OBJECT/DO_LARGE_OBJECT_DATA (object type constants)
  - DUMP_COMPONENT_DATA/DUMP_COMPONENT_ACL (component flags)
- Called from (representative examples):
  - [main](../m/main.md) (pg_dump main function)
  - fmtQualifiedDumpable

## Notes and Other Information
- Groups BLOBs by owner and ACL to reduce the number of archive entries and improve efficiency
- Creates separate metadata and data objects to enable proper dependency relationships
- In binary upgrade mode, excludes BLOB data since pg_upgrade copies pg_largeobject table directly
- Uses recordAdditionalCatalogID to allow lookup of LoInfo by any BLOB OID in the group
- Handles both single BLOBs and BLOB ranges in naming (e.g., '12345' vs '12345..12350')
- Essential for proper BLOB backup and restoration in PostgreSQL dumps