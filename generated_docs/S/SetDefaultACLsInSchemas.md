# SetDefaultACLsInSchemas

## Location
src/backend/catalog/aclchk.c: 1161 - 1202

## Overview
Applies default ACL settings to either database-wide scope (when no schemas specified) or to each schema in a provided list of target schemas.

## Definition


## Detailed Description
This function serves as a dispatcher that applies default Access Control List (ACL) settings based on the scope specified. When no schema names are provided (nspnames is NIL), it sets database-wide default privileges by setting the namespace ID to InvalidOid and calling SetDefaultACL. When specific schemas are provided, it iterates through each schema name, resolves the schema name to its OID using get_namespace_oid, sets the resolved namespace ID in the InternalDefaultACL structure, and calls SetDefaultACL for each individual schema. The function includes extensive comments explaining why CREATE privilege checking on schemas was removed - it was causing confusion and preventing certain database states from being properly dumped and restored.

## Parameters / Member Variables
- : Pointer to InternalDefaultACL structure containing all ACL details except nspid (which this function fills in)
- : List of schema name strings, or NIL for database-wide default privileges

## Dependencies
- Functions called/Symbols referenced:
  - get_namespace_oid
  - SetDefaultACL
  - strVal (via lfirst)
- Called from (representative examples):
  - ExecAlterDefaultPrivilegesStmt
  - InternalDefaultACL (internal usage)

## Notes and Other Information
- The function is static and used internally within aclchk.c as part of the default privileges implementation
- When nspnames is NIL, InvalidOid is used as the namespace ID to indicate database-wide default privileges
- The function previously included CREATE privilege checking on target schemas, but this was removed due to usability and dump/restore issues
- Schema name resolution is performed with 'missing_ok = false', meaning an error will be thrown if a schema doesn't exist
- The InternalDefaultACL structure is modified in-place by setting the nspid field before delegating to SetDefaultACL
- Each schema is processed independently, allowing for fine-grained control over default privileges per schema