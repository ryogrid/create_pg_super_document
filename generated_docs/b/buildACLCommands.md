# buildACLCommands

## Location
src/bin/pg_dump/dumputils.c: 102 - 363

## Overview
The buildACLCommands function generates GRANT and REVOKE SQL commands for PostgreSQL database objects by comparing actual ACL (Access Control List) strings with base ACL permissions.

## Definition


## Detailed Description
This function is the core ACL command builder in pg_dump utilities. It takes an object's current ACL string and compares it with the base (default) ACL to determine what GRANT and REVOKE commands need to be generated to recreate the object's permissions in a restored database.

The function performs differential analysis - it identifies privileges that need to be granted (present in actual ACL but not in base ACL) and privileges that need to be revoked (present in base ACL but not in actual ACL). This approach ensures that only the necessary permission changes are dumped, making the output more efficient and readable.

The function handles special cases including:
- Objects with default permissions (null ACLs)
- Owner grants appearing before public grants for consistency
- Grantor changes requiring SET SESSION AUTHORIZATION
- Privileges with and without GRANT OPTION
- Namespace-qualified object names

## Parameters / Member Variables
- : The object name, already quoted and formatted for use in commands
- : The sub-object name if any (e.g., column name), already quoted; NULL if none
- : The namespace the object is in; NULL if none, not pre-quoted
- : The object type for GRANT command (TABLE, SEQUENCE, FUNCTION, etc.)
- : The current ACL string fetched from the database
- : The initial/default ACL string for this object type and owner
- : Username of object owner (will be passed through fmtId); can be NULL
- : String to prefix to each generated command; typically empty or "ALTER DEFAULT PRIVILEGES "
- : Version of the source database
- : PQExpBuffer to append the generated SQL commands to

## Dependencies
- Functions called/Symbols referenced:
  - [parsePGArray](../p/parsePGArray.md) (for parsing ACL arrays)
  - [parseAclItem](../p/parseAclItem.md) (for parsing individual ACL entries)
  - pg_malloc (for memory allocation)
  - [fmtId](../f/fmtId.md) (for identifier quoting)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md) (for string formatting)
  - createPQExpBuffer/destroyPQExpBuffer (for buffer management)

- Called from (representative examples):
  - [buildDefaultACLCommands](buildDefaultACLCommands.md) (in dumputils.c)
  - [dumpACL](../d/dumpACL.md) (in pg_dump.c)
  - [dumpRoleGUCPrivs](../d/dumpRoleGUCPrivs.md) (in pg_dumpall.c)
  - [dumpTablespaces](../d/dumpTablespaces.md) (in pg_dumpall.c)

## Notes and Other Information
- Returns true on success, false if ACL parsing fails
- The function preserves the order of privileges as they appear in the ACL string to maintain GRANT WITH GRANT OPTION dependencies
- Owner grants are always output before other grants for consistency
- When grantor differs from owner, SET SESSION AUTHORIZATION commands are generated
- The baseacls parameter can be either acldefault() result or pg_init_privs entry
- Special handling for default ACL processing where name can be empty string
- Location: src/bin/pg_dump/dumputils.c:102-363