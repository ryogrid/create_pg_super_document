# SetDefaultACLsInSchemas

## Location
[src/backend/catalog/aclchk.c:1161-1202](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L1161-L1202)

## Overview
Applies default ACL settings to either database-wide scope (when no schemas specified) or to each schema in a provided list of target schemas.

## Definition

```c
static void
SetDefaultACLsInSchemas(InternalDefaultACL *iacls, List *nspnames)
```
## Detailed Description
This function serves as a dispatcher that applies default Access Control List (ACL) settings based on the scope specified. When no schema names are provided (nspnames is NIL), it sets database-wide default privileges by setting the namespace ID to InvalidOid and calling SetDefaultACL. When specific schemas are provided, it iterates through each schema name, resolves the schema name to its OID using get_namespace_oid, sets the resolved namespace ID in the InternalDefaultACL structure, and calls SetDefaultACL for each individual schema. The function includes extensive comments explaining why CREATE privilege checking on schemas was removed - it was causing confusion and preventing certain database states from being properly dumped and restored.

## Parameters / Member Variables
- `*iacls`: Pointer to InternalDefaultACL structure containing all ACL details except nspid (which this function fills in)
- `*nspnames`: List of schema name strings, or NIL for database-wide default privileges
## Dependencies
- Functions called/Symbols referenced:
  - [get_namespace_oid](../g/get_namespace_oid.md)
  - [SetDefaultACL](SetDefaultACL.md)
  - strVal (via lfirst)
- Called from (representative examples):
  - [ExecAlterDefaultPrivilegesStmt](../E/ExecAlterDefaultPrivilegesStmt.md)
  - InternalDefaultACL (internal usage)

## Notes and Other Information
- The function is static and used internally within aclchk.c as part of the default privileges implementation
- When nspnames is NIL, InvalidOid is used as the namespace ID to indicate database-wide default privileges
- The function previously included CREATE privilege checking on target schemas, but this was removed due to usability and dump/restore issues
- Schema name resolution is performed with 'missing_ok = false', meaning an error will be thrown if a schema doesn't exist
- The InternalDefaultACL structure is modified in-place by setting the nspid field before delegating to SetDefaultACL
- Each schema is processed independently, allowing for fine-grained control over default privileges per schema

## Simplified Source

```c
static void
SetDefaultACLsInSchemas(InternalDefaultACL *iacls, List *nspnames)
{
    if (nspnames == NIL)
    {
        // Set database-wide permissions when no schema specified
        iacls->nspid = InvalidOid;
        SetDefaultACL(iacls);
    }
    else
    {
        // Apply permissions to each specified schema
        ListCell *nspcell;

        foreach(nspcell, nspnames)
        {
            char *nspname = strVal(lfirst(nspcell));

            // Resolve schema name to OID
            iacls->nspid = get_namespace_oid(nspname, false);

            // Apply ACL to this schema
            SetDefaultACL(iacls);
        }
    }
}
```