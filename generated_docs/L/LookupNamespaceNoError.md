# LookupNamespaceNoError

## Location
[src/backend/catalog/namespace.c:3355-3384](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L3355-L3384)

## Overview
Looks up a schema name and returns its namespace OID, returning InvalidOid if the namespace is not found (without raising an error).

## Definition
```c
Oid LookupNamespaceNoError(const char *nspname)
```

## Detailed Description
LookupNamespaceNoError provides a non-error-throwing way to look up a namespace by name. It handles the special "pg_temp" alias for the current session's temporary namespace, returning the temporary namespace OID if it exists or InvalidOid if no temporary namespace has been created yet. For regular namespace names, it delegates to get_namespace_oid with the missing_ok flag set to true. This function performs no permission checks - callers are responsible for ensuring appropriate authorization.

## Parameters / Member Variables
- `nspname`: The name of the namespace/schema to look up

## Dependencies
- Functions called/Symbols referenced:
  - strcmp
  - OidIsValid
  - InvokeNamespaceSearchHook
  - [get_namespace_oid](../g/get_namespace_oid.md)
- Called from (representative examples):
  - [schema_does_not_exist_skipping](../s/schema_does_not_exist_skipping.md)
  - [DropErrorMsgNonExistent](../D/DropErrorMsgNonExistent.md)
  - [refnameNamespaceItem](../r/refnameNamespaceItem.md)

## Notes and Other Information
- Returns InvalidOid instead of throwing an error when the namespace is not found
- Does NOT perform permission checks - callers must handle authorization
- Handles the "pg_temp" special alias for temporary namespaces
- For temporary namespaces, does not attempt to initialize them if they don't exist
- [LookupExplicitNamespace](LookupExplicitNamespace.md) is preferable in most cases as it includes permission checks
- Part of PostgreSQL's namespace lookup infrastructure
- Located in src/backend/catalog/namespace.c:3355-3384

## Simplified Source

```c
Oid
LookupNamespaceNoError(const char *nspname)
{
    // Handle special "pg_temp" alias for temporary namespace
    if (strcmp(nspname, "pg_temp") == 0) {
        if (OidIsValid(myTempNamespace)) {
            InvokeNamespaceSearchHook(myTempNamespace, true);
            return myTempNamespace;
        }
        // Don't try to initialize temp namespace, just return not found
        return InvalidOid;
    }

    // Look up regular namespace (missing_ok = true means no error on not found)
    return get_namespace_oid(nspname, true);
}
```