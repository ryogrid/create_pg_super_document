# selectDumpableNamespace

## Location
[src/bin/pg_dump/pg_dump.c:1784-1869](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L1784-L1869)

## Overview
Policy-setting function that determines which components of a namespace (schema) should be dumped based on dump options, schema type, inclusion/exclusion lists, and extension membership.

## Definition
```c
static void selectDumpableNamespace(NamespaceInfo *nsinfo, Archive *fout)
```

## Detailed Description
This function implements complex logic to determine what parts of a namespace should be included in a database dump. The decision-making process follows this hierarchy:

1. **Table-specific dumps**: If specific tables are being dumped, no complete namespaces are dumped
2. **Schema-specific dumps**: If specific schemas are listed, only those are dumped
3. **System schema handling**:
   - pg_catalog: In PostgreSQL 9.6+, dumps ACLs only (not original initdb ACLs)
   - Other pg_* and information_schema: Not dumped
4. **Public schema**: Special handling due to its unique status - sets create=false and handles ownership-based decisions
5. **User schemas**: All components dumped by default
6. **Exclusion overrides**: Schema exclusion lists can override any dump decision
7. **Extension membership**: Checked last, can override schema dump decision but not contents

The function sets both dump (what to dump for the schema itself) and dump_contains (what to dump for objects within the schema) flags appropriately.

## Parameters / Member Variables
- `nsinfo`: Pointer to NamespaceInfo structure containing schema information and dump flags to be set
- `fout`: Pointer to Archive structure containing dump options and PostgreSQL version information

## Dependencies
- Functions called/Symbols referenced:
  - [simple_oid_list_member](simple_oid_list_member.md) (check membership in include/exclude lists)
  - [checkExtensionMembership](../c/checkExtensionMembership.md) (check if schema belongs to extension)
  - Various DUMP_COMPONENT_* constants (NONE, ALL, ACL, DEFINITION, COMMENT)
- Called from (representative examples):
  - [getNamespaces](../g/getNamespaces.md)

## Notes and Other Information
- This is a static function within pg_dump.c used during the dump planning phase
- The public schema receives special treatment due to its hybrid system/user nature
- PostgreSQL 9.6+ introduced pg_init_privs, allowing selective ACL dumping for system catalogs
- The function handles the distinction between dumping the schema definition itself vs. dumping objects contained within it
- Extension membership is checked last and can override schema-level dump decisions
- The create flag determines whether CREATE SCHEMA statements are generated

## Simplified Source

```c
static void
selectDumpableNamespace(NamespaceInfo *nsinfo, Archive *fout)
{
    // Default: create schema if we're dumping its definition
    nsinfo->create = true;

    // Determine dump policy based on inclusion lists and schema type
    if (table_include_oids.head != NULL)
    {
        // Specific tables being dumped - don't dump complete namespaces
        nsinfo->dobj.dump_contains = nsinfo->dobj.dump = DUMP_COMPONENT_NONE;
    }
    else if (schema_include_oids.head != NULL)
    {
        // Specific schemas being dumped - check if this one is included
        nsinfo->dobj.dump_contains = nsinfo->dobj.dump =
            simple_oid_list_member(&schema_include_oids, nsinfo->dobj.catId.oid) ?
            DUMP_COMPONENT_ALL : DUMP_COMPONENT_NONE;
    }
    else if (fout->remoteVersion >= 90600 &&
             strcmp(nsinfo->dobj.name, "pg_catalog") == 0)
    {
        // PostgreSQL 9.6+: dump ACLs only for pg_catalog (not original initdb ACLs)
        nsinfo->dobj.dump_contains = nsinfo->dobj.dump = DUMP_COMPONENT_ACL;
    }
    else if (strncmp(nsinfo->dobj.name, "pg_", 3) == 0 ||
             strcmp(nsinfo->dobj.name, "information_schema") == 0)
    {
        // Other system schemas don't get dumped
        nsinfo->dobj.dump_contains = nsinfo->dobj.dump = DUMP_COMPONENT_NONE;
    }
    else if (strcmp(nsinfo->dobj.name, "public") == 0)
    {
        // Special handling for public schema
        nsinfo->create = false;  // CREATE SCHEMA would fail
        nsinfo->dobj.dump = DUMP_COMPONENT_ALL;

        // Omit definition if owner is default
        if (nsinfo->nspowner == ROLE_PG_DATABASE_OWNER)
            nsinfo->dobj.dump &= ~DUMP_COMPONENT_DEFINITION;

        nsinfo->dobj.dump_contains = DUMP_COMPONENT_ALL;
        nsinfo->dobj.components |= DUMP_COMPONENT_COMMENT;  // Force comment handling
    }
    else
    {
        // User schemas: dump all components
        nsinfo->dobj.dump_contains = nsinfo->dobj.dump = DUMP_COMPONENT_ALL;
    }

    // Apply exclusion list overrides
    if (nsinfo->dobj.dump_contains &&
        simple_oid_list_member(&schema_exclude_oids, nsinfo->dobj.catId.oid))
        nsinfo->dobj.dump_contains = nsinfo->dobj.dump = DUMP_COMPONENT_NONE;

    // Check extension membership (can override schema dump decision)
    (void) checkExtensionMembership(&nsinfo->dobj, fout);
}
```