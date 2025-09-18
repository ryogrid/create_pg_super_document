# selectDumpableNamespace

## Location
src/bin/pg_dump/pg_dump.c: 1784 - 1869

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
  - simple_oid_list_member (check membership in include/exclude lists)
  - checkExtensionMembership (check if schema belongs to extension)
  - Various DUMP_COMPONENT_* constants (NONE, ALL, ACL, DEFINITION, COMMENT)
- Called from (representative examples):
  - getNamespaces

## Notes and Other Information
- This is a static function within pg_dump.c used during the dump planning phase
- The public schema receives special treatment due to its hybrid system/user nature
- PostgreSQL 9.6+ introduced pg_init_privs, allowing selective ACL dumping for system catalogs
- The function handles the distinction between dumping the schema definition itself vs. dumping objects contained within it
- Extension membership is checked last and can override schema-level dump decisions
- The create flag determines whether CREATE SCHEMA statements are generated