# buildDefaultACLCommands

## Location
src/bin/pg_dump/dumputils.c: 364 - 420

## Overview
The buildDefaultACLCommands function generates ALTER DEFAULT PRIVILEGES SQL commands for PostgreSQL pg_default_acl entries, which control default permissions for newly created objects.

## Definition


## Detailed Description
This function is a specialized wrapper around buildACLCommands that specifically handles default ACL entries from the pg_default_acl system catalog. Default ACLs control what permissions are automatically granted to newly created objects of a specific type within a schema or globally.

The function constructs an appropriate "ALTER DEFAULT PRIVILEGES" command prefix and delegates the actual ACL comparison and command generation to buildACLCommands. It handles both schema-specific and global default privileges by including or omitting the "IN SCHEMA" clause as needed.

The function incorporates the target role directly into the command rather than using SET ROLE, ensuring that permission errors result in no changes rather than changing default privileges for the wrong user.

## Parameters / Member Variables
- : The object type for default privileges (TABLES, FUNCTIONS, SEQUENCES, etc.)
- : Schema name for schema-specific default privileges, or NULL for global defaults
- : The ACL string fetched from the pg_default_acl table
- : The appropriate default ACL for the specified object type and owner
- : Username of the privileges owner (will be formatted through fmtId)
- : Version of the source database
- : PQExpBuffer to append the generated ALTER DEFAULT PRIVILEGES commands to

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer/destroyPQExpBuffer (for buffer management)
  - [fmtId](../f/fmtId.md) (for identifier quoting)
  - [buildACLCommands](buildACLCommands.md) (for actual ACL command generation)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md) (for string building)

- Called from (representative examples):
  - [dumpDefaultACL](../d/dumpDefaultACL.md) (in pg_dump.c)

## Notes and Other Information
- Returns true on success, false if ACL parsing fails (delegated to buildACLCommands)
- There are no initprivs (initial privileges) for default ACLs, so the base ACL is always the object-type-specific default
- The function passes empty strings for name and subname to buildACLCommands since default ACLs apply to object types, not specific objects
- Default privileges can be set either globally (when nspname is NULL) or for a specific schema
- The resulting commands modify what permissions are granted by default for future object creation
- Location: src/bin/pg_dump/dumputils.c:364-420