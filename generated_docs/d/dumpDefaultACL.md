# dumpDefaultACL

## Location
[src/bin/pg_dump/pg_dump.c:15173-15261](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L15173-L15261)

## Overview
Generates ALTER DEFAULT PRIVILEGES statements to recreate default access control lists for various database object types during database restoration.

## Definition

```c
static void
dumpDefaultACL(Archive *fout, const DefaultACLInfo *daclinfo)
```
## Detailed Description
The  function handles the dumping of default privileges (default ACLs) for database objects. Default privileges allow database administrators to set up access permissions that will be automatically applied to newly created objects of specific types within a schema or database.

The function performs the following key operations:
1. **Object type mapping** - Converts internal object type constants to human-readable strings (TABLES, SEQUENCES, FUNCTIONS, TYPES, SCHEMAS)
2. **Command generation** - Uses  to construct the appropriate ALTER DEFAULT PRIVILEGES statements
3. **Conditional dumping** - Respects dump options for data-only dumps and ACL skipping
4. **Archive integration** - Creates archive entries in the POST_DATA section to ensure proper restoration order

Default privileges are particularly important in multi-user environments where consistent permission schemes need to be maintained across object creation.

## Parameters / Member Variables
- : Archive structure containing dump configuration and output methods
- : DefaultACLInfo structure containing the default privileges information, including object type, role, namespace, ACL data, and dump flags

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer
  - [buildDefaultACLCommands](../b/buildDefaultACLCommands.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [pg_fatal](../p/pg_fatal.md)
  - destroyPQExpBuffer
  - DEFACLOBJ_RELATION, DEFACLOBJ_SEQUENCE, DEFACLOBJ_FUNCTION, DEFACLOBJ_TYPE, DEFACLOBJ_NAMESPACE constants
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md)
  - fmtQualifiedDumpable

## Notes and Other Information
- The function includes safety checks and will terminate with a fatal error if an unrecognized object type is encountered
- Default privileges are dumped in the POST_DATA section to ensure they are applied after all objects have been created
- The function respects both  and  dump options, allowing users to exclude default privileges from dumps when needed
- Default privileges can be scoped to specific schemas or apply database-wide (when namespace is NULL)
- The actual SQL command construction is delegated to , which handles the complex logic of comparing current vs. default privileges