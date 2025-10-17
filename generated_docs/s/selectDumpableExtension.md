# selectDumpableExtension

## Location
[src/bin/pg_dump/pg_dump.c:2069-2107](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L2069-L2107)

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
  - [simple_oid_list_member](simple_oid_list_member.md)
  - [ExtensionInfo](../E/ExtensionInfo.md) (structure)
  - DumpOptions (structure)
  - DUMP_COMPONENT_ACL (constant)
  - DUMP_COMPONENT_ALL (constant)
  - DUMP_COMPONENT_NONE (constant)
  - extension_include_oids (global variable)
  - extension_exclude_oids (global variable)
  - g_last_builtin_oid (global variable)
- Called from (representative examples):
  - [getExtensions](../g/getExtensions.md)

## Notes and Other Information
- Built-in extensions are identified by OID range and treated specially since they're assumed to exist in target databases
- The function supports both explicit inclusion (--extension) and exclusion lists
- Both dump and dump_contains are set to the same value to ensure consistency
- Exclude lists take precedence over include lists and other dump policies
- The function handles the complex interaction between schema/table-specific dumps and extension dumps
- ACL-only dumping for built-in extensions allows preservation of permission changes
- The function is static and only used internally within pg_dump.c

## Simplified Source

```c
static void
selectDumpableExtension(ExtensionInfo *extinfo, DumpOptions *dopt)
{
    // Built-in extensions: only dump ACLs to preserve permission changes
    if (extinfo->dobj.catId.oid <= (Oid) g_last_builtin_oid) {
        extinfo->dobj.dump = extinfo->dobj.dump_contains = DUMP_COMPONENT_ACL;
        return;
    }

    // User-defined extensions: check inclusion/exclusion lists
    if (extension_include_oids.head != NULL) {
        // Explicit include list exists: only dump if listed
        extinfo->dobj.dump = extinfo->dobj.dump_contains =
            simple_oid_list_member(&extension_include_oids, extinfo->dobj.catId.oid) ?
            DUMP_COMPONENT_ALL : DUMP_COMPONENT_NONE;
    } else {
        // No include list: dump based on include_everything setting
        extinfo->dobj.dump = extinfo->dobj.dump_contains =
            dopt->include_everything ?
            DUMP_COMPONENT_ALL : DUMP_COMPONENT_NONE;
    }

    // Apply exclude list - overrides all other settings
    if (extinfo->dobj.dump &&
        simple_oid_list_member(&extension_exclude_oids, extinfo->dobj.catId.oid))
        extinfo->dobj.dump = extinfo->dobj.dump_contains = DUMP_COMPONENT_NONE;
}
```