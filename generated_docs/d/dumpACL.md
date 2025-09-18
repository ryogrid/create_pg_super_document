# dumpACL

## Location
src/bin/pg_dump/pg_dump.c: 15262 - 15389

## Overview
Generates GRANT and REVOKE statements necessary to recreate access control lists (ACLs) for database objects during database restoration.

## Definition


## Detailed Description
The  function is a central component of pg_dump's ACL handling system. It generates the SQL statements needed to recreate the access control permissions for various database objects during database restoration. The function handles complex permission scenarios including:

1. **Permission state transitions** - Calculates the difference between default permissions and current permissions to generate minimal GRANT/REVOKE commands
2. **Binary upgrade support** - Special handling for extension objects that need to preserve initial privileges in pg_init_privs during binary upgrades
3. **Dependency management** - Creates proper dependency relationships between the ACL entry and the underlying object(s)
4. **Multiple object types** - Supports ACLs for tables, sequences, functions, languages, schemas, databases, tablespaces, foreign data wrappers, servers, and large objects

The function uses  to perform the complex logic of comparing ACL states and generating the appropriate SQL commands. It only creates archive entries when there are actual permission changes to apply.

## Parameters / Member Variables
- : Archive structure for output generation and configuration
- : Dump ID of the primary object whose ACL is being dumped
- : Optional secondary dump ID for additional dependencies, or InvalidDumpId
- : Object type string (TABLE, SEQUENCE, FUNCTION, etc.)
- : Properly formatted and quoted object name
- : Formatted sub-object name (typically for table columns), or NULL
- : Namespace/schema name, or NULL for global objects
- : Custom tag for the ACL TOC entry, or NULL to use default
- : Object owner name, or NULL for ownerless objects like languages
- : DumpableAcl structure containing current ACL, default ACL, privilege type, and initial privileges

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer
  - buildACLCommands
  - createDumpId
  - ArchiveEntry
  - pg_fatal
  - destroyPQExpBuffer
  - InvalidDumpId constant
- Called from (representative examples):
  - dumpTable
  - dumpFunc
  - dumpNamespace
  - dumpForeignServer
  - dumpDatabase

## Notes and Other Information
- Returns the dump ID of the created ACL entry, or InvalidDumpId if no ACL entry was needed
- Respects  option to allow dumps without ACL information
- In data-only dumps, only large object ACLs are included (all other ACLs are skipped)
- Binary upgrade mode includes special logic to preserve pg_init_privs contents for extension objects
- The function handles the case where a NULL/empty ACL string represents the object-type-specific default
- ACL entries are created in SECTION_NONE, allowing them to be applied at the appropriate time during restoration
- Proper dependency tracking ensures ACLs are applied after the underlying objects exist