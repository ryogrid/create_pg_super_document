# preprocessNamespacePath

## Location
[src/backend/catalog/namespace.c:4107-4197](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L4107-L4197)

## Overview
Converts a comma-separated namespace search path string into a list of namespace OIDs, performing access control checks and resolving special namespace references.

## Definition
```c
static List *preprocessNamespacePath(const char *searchPath, Oid roleid, bool *temp_missing)
```

## Detailed Description
This function parses a comma-separated string of namespace names and converts them to a list of valid namespace OIDs. It handles special namespace references like `` (which expands to a namespace matching the user's role name) and `pg_temp` (which expands to the session's temporary namespace). For each namespace, it performs ACL checks to ensure the specified role has USAGE permission. Invalid or inaccessible namespaces are silently excluded from the result list. The function also tracks whether the temporary namespace was missing when it should have been the creation namespace.

## Parameters / Member Variables
- `searchPath`: Comma-separated string of namespace names to process
- `roleid`: OID of the role for which to check access permissions
- `temp_missing`: Output parameter set to true if pg_temp was first in path but no temp namespace exists

## Dependencies
- Functions called/Symbols referenced:
  - [SplitIdentifierString](../S/SplitIdentifierString.md)
  - Form_pg_authid
  - [get_namespace_oid](../g/get_namespace_oid.md)
  - [object_aclcheck](../o/object_aclcheck.md)
  - ACL_USAGE
  - [lappend_oid](../l/lappend_oid.md)
  - [list_free](../l/list_free.md)
- Called from (representative examples):
  - [cachedNamespacePath](../c/cachedNamespacePath.md)

## Notes and Other Information
- Silently excludes namespaces that don't exist or are inaccessible to avoid errors
- Handles special namespace references: `` expands to role-named namespace, `pg_temp` to temp namespace
- Performs ACL_USAGE permission checks for each namespace
- Sets temp_missing flag when pg_temp is first in path but no temporary namespace exists
- Returns a newly-allocated list that must be freed by the caller
- Does not allow duplicate namespace entries in the result list

## Simplified Source

```c
static List *preprocessNamespacePath(const char *searchPath, Oid roleid, bool *temp_missing) {
    char *rawname = pstrdup(searchPath);
    List *namelist;
    List *oidlist = NIL;

    // Parse comma-separated string into identifier list
    if (!SplitIdentifierString(rawname, ',', &namelist))
        elog(ERROR, "invalid list syntax");

    *temp_missing = false;

    // Convert names to OIDs with access checks
    foreach(lc, namelist) {
        char *curname = (char *) lfirst(lc);
        Oid namespaceId;

        if (strcmp(curname, "$user") == 0) {
            // Handle $user special reference
            HeapTuple tuple = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));
            if (HeapTupleIsValid(tuple)) {
                char *rname = NameStr(((Form_pg_authid) GETSTRUCT(tuple))->rolname);
                namespaceId = get_namespace_oid(rname, true);
                ReleaseSysCache(tuple);

                if (OidIsValid(namespaceId) &&
                    object_aclcheck(NamespaceRelationId, namespaceId, roleid, ACL_USAGE) == ACLCHECK_OK)
                    oidlist = lappend_oid(oidlist, namespaceId);
            }
        }
        else if (strcmp(curname, "pg_temp") == 0) {
            // Handle pg_temp special reference
            if (OidIsValid(myTempNamespace))
                oidlist = lappend_oid(oidlist, myTempNamespace);
            else if (oidlist == NIL)
                *temp_missing = true;
        }
        else {
            // Handle normal namespace name
            namespaceId = get_namespace_oid(curname, true);
            if (OidIsValid(namespaceId) &&
                object_aclcheck(NamespaceRelationId, namespaceId, roleid, ACL_USAGE) == ACLCHECK_OK)
                oidlist = lappend_oid(oidlist, namespaceId);
        }
    }

    // Clean up
    pfree(rawname);
    list_free(namelist);

    return oidlist;
}
```