# selectDumpableProcLang

## Location
src/bin/pg_dump/pg_dump.c: 2001 - 2033

## Overview
This function determines whether a procedural language should be included in a PostgreSQL database dump, with special handling for built-in languages and version-specific ACL support.

## Definition
```c
static void selectDumpableProcLang(ProcLangInfo *plang, Archive *fout)
```

## Detailed Description
The selectDumpableProcLang function implements the policy logic for deciding whether to include procedural languages in database dumps. Procedural languages do not belong to any particular namespace, so the function uses OID ranges and version checks to make appropriate decisions.

The function operates with the following logic:

1. **Extension membership**: If the language is part of an extension, that overrides all other considerations
2. **Include everything check**: Procedural languages are only considered when dumping everything (include_everything option)
3. **Built-in vs user-defined languages**: 
   - Built-in languages (OID ≤ g_last_builtin_oid) are handled like objects in pg_catalog namespace
   - For PostgreSQL 9.6+, built-in languages include only ACLs (DUMP_COMPONENT_ACL)
   - For older versions, built-in languages are not dumped at all
   - User-defined languages (higher OIDs) are fully dumped (DUMP_COMPONENT_ALL)

This approach ensures that system languages are handled appropriately while preserving user-defined languages and their associated permissions.

## Parameters / Member Variables
- `plang`: Pointer to ProcLangInfo structure containing information about the procedural language being evaluated
- `fout`: Pointer to Archive structure containing dump context and options, including remote server version

## Dependencies
- Functions called/Symbols referenced:
  - [checkExtensionMembership](../c/checkExtensionMembership.md)
  - [ProcLangInfo](../P/ProcLangInfo.md) (structure)
  - DUMP_COMPONENT_NONE (constant)
  - DUMP_COMPONENT_ACL (constant)
  - DUMP_COMPONENT_ALL (constant)
  - g_last_builtin_oid (global variable)
- Called from (representative examples):
  - [getProcLangs](../g/getProcLangs.md)

## Notes and Other Information
- Procedural languages do not live in any namespace, making them similar to casts in this regard
- The function includes version-specific logic for PostgreSQL 9.6+, which introduced ACL support for built-in languages
- Extension membership always takes precedence over other dump policies
- Only dumped when include_everything is enabled, unlike some other object types
- The function is static and only used internally within pg_dump.c