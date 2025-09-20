# IsReservedName

## Location
[src/backend/catalog/catalog.c:247-272](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/catalog.c#L247-L272)

## Overview
IsReservedName is a utility function that determines whether a given name starts with the reserved "pg_" prefix used for PostgreSQL system objects.

## Definition

```c
bool
IsReservedName(const char *name)
```
## Detailed Description
This function performs a simple but critical check to determine if an object name begins with the "pg_" prefix, which is reserved for PostgreSQL system objects. The function is optimized for speed with direct character comparisons rather than string functions. The pg_ prefix reservation applies to different classes of objects including schemas, tablespaces (as of version 8.0), and roles (as of version 9.6). This naming convention helps prevent conflicts between user-defined objects and system objects.

## Parameters / Member Variables
- : A null-terminated string containing the object name to check

## Dependencies
- Functions called/Symbols referenced: None (uses only basic character comparisons)
- Called from (representative examples):
  - [CreateSchemaCommand](../C/CreateSchemaCommand.md) (src/backend/commands/schemacmds.c:106)
  - [CreateRole](../C/CreateRole.md) (src/backend/commands/user.c:351)
  - [CreateTableSpace](../C/CreateTableSpace.md) (src/backend/commands/tablespace.c:280)
  - [RenameSchema](../R/RenameSchema.md) (src/backend/commands/schemacmds.c:286)
  - [RenameRole](../R/RenameRole.md) (src/backend/commands/user.c:1383, 1390)
  - [RenameTableSpace](../R/RenameTableSpace.md) (src/backend/commands/tablespace.c:967)
  - [pg_replication_origin_create](../p/pg_replication_origin_create.md) (src/backend/replication/logical/origin.c:1282)
  - check_rolespec_name (src/backend/utils/adt/acl.c:5586)

## Notes and Other Information
- The function uses direct character array indexing for performance optimization
- Returns true if the name starts with exactly "pg_", false otherwise
- This check is essential for maintaining the separation between system and user objects
- The reserved prefix policy has evolved over PostgreSQL versions, expanding from schemas and tablespaces to include roles