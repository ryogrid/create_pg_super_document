# LookupExplicitNamespace

## Location
[src/backend/catalog/namespace.c:3385-3427](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L3385-L3427)

## Overview  
Processes an explicitly-specified schema name by looking up the schema and verifying the current user has USAGE (lookup) rights in it.

## Definition
```c
Oid LookupExplicitNamespace(const char *nspname, bool missing_ok)
```

## Detailed Description
LookupExplicitNamespace provides a secure way to look up a namespace by name with proper permission checking. It handles the special "pg_temp" alias for temporary namespaces, looks up the namespace OID using get_namespace_oid, and then performs an ACL (Access Control List) check to ensure the current user has USAGE privileges on the namespace. If the permission check fails, it raises an appropriate error. The function also invokes the namespace search hook for auditing or extension purposes.

## Parameters / Member Variables
- `nspname`: The name of the namespace/schema to look up
- `missing_ok`: If true, return InvalidOid for non-existent namespaces instead of raising an error

## Dependencies
- Functions called/Symbols referenced:
  - strcmp
  - OidIsValid  
  - [get_namespace_oid](../g/get_namespace_oid.md)
  - [object_aclcheck](../o/object_aclcheck.md)
  - [GetUserId](../G/GetUserId.md)
  - [aclcheck_error](../a/aclcheck_error.md)
  - InvokeNamespaceSearchHook
- Called from (representative examples):
  - [objectsInSchemaToOids](../o/objectsInSchemaToOids.md)
  - [RangeVarGetRelidExtended](../R/RangeVarGetRelidExtended.md)
  - [FuncnameGetCandidates](../F/FuncnameGetCandidates.md)
  - [OpernameGetOprid](../O/OpernameGetOprid.md)
  - [LookupTypeNameExtended](LookupTypeNameExtended.md)

## Notes and Other Information
- Includes proper permission checking via ACL_USAGE privilege verification
- Handles the "pg_temp" special alias for temporary namespaces
- Does not attempt to initialize temporary namespaces if they don't exist
- Raises permission errors if the user lacks USAGE rights on the namespace
- Invokes namespace search hooks for extensibility and auditing
- This is the preferred function for most namespace lookups due to its security checks
- Widely used throughout PostgreSQL for secure namespace resolution
- Located in src/backend/catalog/namespace.c:3385-3427