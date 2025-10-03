# dumpACL

## Location
[src/bin/pg_dump/pg_dump.c:15262-15389](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L15262-L15389)

## Overview
Generates GRANT and REVOKE statements necessary to recreate access control lists (ACLs) for database objects during database restoration.

## Definition

```c
static DumpId
dumpACL(Archive *fout, DumpId objDumpId, DumpId altDumpId,
		const char *type, const char *name, const char *subname,
		const char *nspname, const char *tag, const char *owner,
		const DumpableAcl *dacl)
```
## Detailed Description
The  function is a central component of pg_dump's ACL handling system. It generates the SQL statements needed to recreate the access control permissions for various database objects during database restoration. The function handles complex permission scenarios including:

1. **Permission state transitions** - Calculates the difference between default permissions and current permissions to generate minimal GRANT/REVOKE commands
2. **Binary upgrade support** - Special handling for extension objects that need to preserve initial privileges in pg_init_privs during binary upgrades
3. **Dependency management** - Creates proper dependency relationships between the ACL entry and the underlying object(s)
4. **Multiple object types** - Supports ACLs for tables, sequences, functions, languages, schemas, databases, tablespaces, foreign data wrappers, servers, and large objects

The function uses  to perform the complex logic of comparing ACL states and generating the appropriate SQL commands. It only creates archive entries when there are actual permission changes to apply.

## Parameters / Member Variables
- `*fout`: Archive structure for output generation and configuration
- `objDumpId`: Dump ID of the primary object whose ACL is being dumped
- `altDumpId`: Optional secondary dump ID for additional dependencies, or InvalidDumpId
- `*type`: Object type string (TABLE, SEQUENCE, FUNCTION, etc.)
- `*name`: Properly formatted and quoted object name
- `*subname`: Formatted sub-object name (typically for table columns), or NULL
- `*nspname`: Namespace/schema name, or NULL for global objects
- `*tag`: Custom tag for the ACL TOC entry, or NULL to use default
- `*owner`: Object owner name, or NULL for ownerless objects like languages
- `*dacl`: DumpableAcl structure containing current ACL, default ACL, privilege type, and initial privileges
## Dependencies
- Functions called/Symbols referenced:
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - [buildACLCommands](../b/buildACLCommands.md)
  - [createDumpId](../c/createDumpId.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [pg_fatal](../p/pg_fatal.md)
  - [destroyPQExpBuffer](destroyPQExpBuffer.md)
  - InvalidDumpId constant
- Called from (representative examples):
  - [dumpTable](dumpTable.md)
  - [dumpFunc](dumpFunc.md)
  - [dumpNamespace](dumpNamespace.md)
  - [dumpForeignServer](dumpForeignServer.md)
  - [dumpDatabase](dumpDatabase.md)

## Notes and Other Information
- Returns the dump ID of the created ACL entry, or InvalidDumpId if no ACL entry was needed
- Respects  option to allow dumps without ACL information
- In data-only dumps, only large object ACLs are included (all other ACLs are skipped)
- Binary upgrade mode includes special logic to preserve pg_init_privs contents for extension objects
- The function handles the case where a NULL/empty ACL string represents the object-type-specific default
- ACL entries are created in SECTION_NONE, allowing them to be applied at the appropriate time during restoration
- Proper dependency tracking ensures ACLs are applied after the underlying objects exist