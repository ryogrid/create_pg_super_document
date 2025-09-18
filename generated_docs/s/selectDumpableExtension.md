# selectDumpableExtension

## Location
src/bin/pg_dump/pg_dump.c: 2069 - 2107

## Overview
This function determines whether an extension should be included in a PostgreSQL database dump based on built-in status, explicit inclusion/exclusion lists, and dump options.

## Definition
```c
static void selectDumpableExtension(ExtensionInfo *extinfo, DumpOptions *dopt)
```

## Detailed Description
The selectDumpableExtension function implements the policy logic for deciding whether to include extensions in database dumps. Extensions are packages of SQL objects that can be installed and managed as a unit. The function handles complex logic involving built-in vs user-defined extensions, explicit include/exclude lists, and general dump policies.

The function operates with the following logic:

1. **Built-in extensions**: Extensions with OIDs in the initdb range are assumed to be pre-installed in target databases, so only their ACLs are dumped to preserve permission changes
2. **User-defined extensions**: For extensions with higher OIDs:
   - If an explicit extension include list exists (--extension option), only listed extensions are dumped
   - Otherwise, extensions are dumped based on the include_everything option
   - Extensions in the exclude list are never dumped, regardless of other settings

The function sets both dump and dump_contains flags to the same value, ensuring consistent handling of the extension and its member objects.

## Parameters / Member Variables
- `extinfo`: Pointer to ExtensionInfo structure containing information about the extension being evaluated
- `dopt`: Pointer to DumpOptions structure containing the dump configuration settings

## Dependencies
- Functions called/Symbols referenced:
  - simple_oid_list_member
  - ExtensionInfo (structure)
  - DumpOptions (structure)
  - DUMP_COMPONENT_ACL (constant)
  - DUMP_COMPONENT_ALL (constant)
  - DUMP_COMPONENT_NONE (constant)
  - extension_include_oids (global variable)
  - extension_exclude_oids (global variable)
  - g_last_builtin_oid (global variable)
- Called from (representative examples):
  - getExtensions

## Notes and Other Information
- Built-in extensions are identified by OID range and treated specially since they're assumed to exist in target databases
- The function supports both explicit inclusion (--extension) and exclusion lists
- Both dump and dump_contains are set to the same value to ensure consistency
- Exclude lists take precedence over include lists and other dump policies
- The function handles the complex interaction between schema/table-specific dumps and extension dumps
- ACL-only dumping for built-in extensions allows preservation of permission changes
- The function is static and only used internally within pg_dump.c