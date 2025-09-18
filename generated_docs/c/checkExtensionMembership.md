# checkExtensionMembership

## Location
[src/bin/pg_dump/pg_dump.c:1734-1783](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L1734-L1783)

## Overview
Determines whether a database object is a member of an extension and configures appropriate dump settings and dependencies based on extension membership status.

## Definition
```c
static bool checkExtensionMembership(DumpableObject *dobj, Archive *fout)
```

## Detailed Description
This function is a central component of pg_dump's extension handling logic. It checks if a given database object belongs to an extension by calling findOwningExtension(). If the object is an extension member, it:

1. Marks the object as an extension member (ext_member = true)
2. Records a dependency relationship between the object and its owning extension
3. Sets appropriate dump flags based on PostgreSQL version and dump mode:
   - For binary upgrades: dumps the same components as the extension
   - For PostgreSQL 9.6+: dumps only ACL deltas (changes from initial setup)
   - For pre-9.6: dumps no components (DUMP_COMPONENT_NONE)

The function implements version-specific behavior because PostgreSQL 9.6 introduced pg_init_privs, which allows dumping only the differences between current and initial ACLs for extension objects.

## Parameters / Member Variables
- `dobj`: Pointer to the DumpableObject being checked for extension membership
- `fout`: Pointer to the Archive structure containing dump options and server version information

## Dependencies
- Functions called/Symbols referenced:
  - [findOwningExtension](../f/findOwningExtension.md) (to locate the owning extension)
  - [addObjectDependency](../a/addObjectDependency.md) (to record dependency relationships)
  - DUMP_COMPONENT_NONE, DUMP_COMPONENT_ACL (dump component flags)
- Called from (representative examples):
  - [selectDumpableNamespace](../s/selectDumpableNamespace.md)
  - [selectDumpableTable](../s/selectDumpableTable.md)
  - [selectDumpableType](../s/selectDumpableType.md)
  - [selectDumpableCast](../s/selectDumpableCast.md)
  - [selectDumpableProcLang](../s/selectDumpableProcLang.md)
  - [selectDumpableAccessMethod](../s/selectDumpableAccessMethod.md)
  - [selectDumpablePublicationObject](../s/selectDumpablePublicationObject.md)
  - [selectDumpableStatisticsObject](../s/selectDumpableStatisticsObject.md)
  - [selectDumpableObject](../s/selectDumpableObject.md)

## Notes and Other Information
- This is a static function within pg_dump.c used internally for extension membership processing
- The function handles version compatibility between different PostgreSQL releases
- Extension members are generally not dumped individually, except for ACL changes in 9.6+
- Binary upgrade mode is an exception where all components are dumped to exactly reproduce the database
- The function returns true if the object is an extension member, false otherwise
- Future enhancements might include RLS policies and security labels, but these require additional privileges