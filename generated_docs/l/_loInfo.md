# _loInfo

## Location
[src/bin/pg_dump/pg_dump.h:602-608](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L602-L608)

## Overview
The `_loInfo` structure represents a group of large objects (blobs) that share the same owner and ACL settings in pg_dump, designed to optimize parallelism during database restore operations.

## Definition
```c
typedef struct _loInfo
{
    DumpableObject dobj;
    DumpableAcl dacl;
    const char *rolname;
    int         numlos;
    Oid         looids[FLEXIBLE_ARRAY_MEMBER];
} LoInfo;
```

## Detailed Description
This structure is a specialized part of pg_dump's internal representation for managing large objects (Binary Large Objects - BLOBs). Rather than creating individual dump entries for each large object, pg_dump groups large objects with identical ownership and access control settings into LoInfo structures. This grouping strategy allows for better parallelization during database restoration, as each LoInfo group spawns separate BLOB METADATA and BLOBS (data) TOC entries that can be processed concurrently. The structure uses a flexible array member to efficiently store variable numbers of large object OIDs.

## Parameters / Member Variables
- `dobj`: Base dumpable object information; the components field has DUMP_COMPONENT_COMMENT bit set if any blob in the group has a comment, and similar flags for security labels
- `dacl`: Access control list information shared by all large objects in this group
- `rolname`: Name of the role/user that owns all the large objects in this group
- `numlos`: Count of large objects included in this group
- `looids`: Flexible array containing the OIDs of all large objects that belong to this group

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject
  - DumpableAcl
  - FLEXIBLE_ARRAY_MEMBER
- Called from (representative examples):
  - No direct references found in the indexed codebase

## Notes and Other Information
- This structure is defined in pg_dump.h as part of the pg_dump utility's internal data structures
- The grouping strategy is an optimization technique to improve restore performance for databases with many large objects
- Large objects with different owners or ACL settings will be placed in separate LoInfo groups
- The flexible array member allows efficient memory allocation for variable-sized groups of large object OIDs
- When there are many large objects with the same owner/ACL, they can be divided into multiple LoInfo groups for even better parallelism
- The structure enables pg_dump to create separate TOC entries for metadata and data, allowing concurrent processing during restoration