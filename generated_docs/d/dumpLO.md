# dumpLO

## Location
[src/bin/pg_dump/pg_dump.c:3814-3903](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L3814-L3903)

## Overview
The  function dumps the metadata definition of a large object group, including OIDs, comments, security labels, and ACL permissions.

## Definition

```c
static void
dumpLO(Archive *fout, const LoInfo *loinfo)
```
## Detailed Description
The  function creates dump archive entries for large object metadata. It generates a simple definition consisting of newline-separated OID lists, then conditionally dumps comments, security labels, and ACL permissions based on the dump component flags. For groups containing multiple BLOBs, it optimizes ACL dumping by creating a single "LARGE OBJECTS" entry that applies to the entire range, while comments and security labels are dumped individually for each BLOB. The function handles both single BLOB and BLOB group scenarios with appropriate naming and tagging strategies.

## Parameters / Member Variables
- : Pointer to the Archive structure representing the output dump file
- : Pointer to the LoInfo structure containing metadata about the large object group, including OIDs, ownership, and ACL information

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer/appendPQExpBuffer/destroyPQExpBuffer (string buffer management)
  - [ArchiveEntry](../A/ArchiveEntry.md) (creates archive entry for BLOB metadata)
  - [dumpComment](dumpComment.md) (dumps individual BLOB comments)
  - [dumpSecLabel](dumpSecLabel.md) (dumps individual BLOB security labels)
  - [dumpACL](dumpACL.md) (dumps ACL permissions for BLOBs)
  - ARCHIVE_OPTS (archive entry configuration macro)
  - DUMP_COMPONENT_* constants (component flags for selective dumping)
  - SECTION_DATA (archive section designation)
  - [CatalogId](../C/CatalogId.md) (catalog identifier structure)
  - InvalidDumpId (null dump ID constant)
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md) (main dump dispatcher function)
  - fmtQualifiedDumpable

## Notes and Other Information
- Creates a simple definition format with newline-separated OIDs for easy parsing during restoration
- Handles comments and security labels individually since they are blob-specific
- Optimizes ACL dumping for groups by creating a single entry that applies to all BLOBs in the range
- Uses conditional dumping based on DUMP_COMPONENT flags to enable selective backup operations
- The "dummy" drop statement is a placeholder since large objects have special drop semantics
- Tag names differentiate between single BLOBs and BLOB ranges for proper restoration handling