# get_user_default_acl

## Location
src/backend/catalog/aclchk.c: 4306 - 4381

## Overview
Retrieves the appropriate default ACL for newly created objects within a given schema by merging global and schema-specific default privileges, returning NULL if built-in system defaults should be used.

## Definition


## Detailed Description
This function implements PostgreSQL's default privilege system by determining the appropriate default ACL for new objects. It supports a two-tier privilege hierarchy: global defaults (applying to all schemas) and schema-specific defaults. The function first maps the object type to the corresponding pg_default_acl encoding, then retrieves both global and schema-specific ACL entries using get_default_acl_internal().

The function performs ACL merging logic where schema-specific permissions take precedence over global ones. If no custom defaults are found, or if the resulting ACL matches the hard-wired system default, it returns NULL to indicate that standard system defaults should be used. This optimization avoids storing redundant ACL data when custom defaults don't actually change permissions.

The function includes a bootstrap mode check to avoid accessing pg_default_acl during system initialization when the catalog might not be available yet.

## Parameters / Member Variables
- : The type of object being created (OBJECT_TABLE, OBJECT_SEQUENCE, OBJECT_FUNCTION, OBJECT_TYPE, OBJECT_SCHEMA)
- : OID of the role that will own the new object
- : OID of the namespace (schema) where the object is being created

## Dependencies
- Functions called/Symbols referenced:
  - IsBootstrapProcessingMode (checks if in bootstrap mode)
  - get_default_acl_internal (retrieves ACL entries from pg_default_acl)
  - acldefault (gets hard-wired default ACL for object type)
  - aclmerge (merges global and schema-specific ACLs)
  - aclitemsort (sorts ACL entries for comparison)
  - aclequal (compares two ACLs for equality)
- Called from:
  - heap_create_with_catalog (for tables and sequences)
  - NamespaceCreate (for schemas)
  - ProcedureCreate (for functions)
  - TypeCreate (for types)

## Notes and Other Information
- Returns NULL during bootstrap mode to avoid catalog access issues
- Only supports object types that have default ACL support in pg_default_acl
- The caller must call recordDependencyOnNewAcl() after object creation if the result is non-NULL
- Performs efficiency optimization by returning NULL when the computed ACL equals system defaults
- Uses a two-tier privilege system: global defaults (InvalidOid namespace) and schema-specific defaults
- Schema-specific privileges override global ones through the aclmerge() operation
- Supported object types are mapped to internal pg_default_acl encoding (e.g., DEFACLOBJ_RELATION for tables)