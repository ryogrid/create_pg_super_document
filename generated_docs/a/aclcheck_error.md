# aclcheck_error

## Location
[src/backend/catalog/aclchk.c:2705-2993](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L2705-L2993)

## Overview
Standardized function for reporting access control check failures, generating appropriate error messages based on the type of permission violation and database object type.

## Definition
```c
void aclcheck_error(AclResult aclerr, ObjectType objtype, const char *objectname)
```

## Detailed Description
This function serves as the central error reporting mechanism for PostgreSQL's access control system. It takes the result of an ACL check and generates user-friendly error messages based on the type of failure and the database object involved. The function handles two main types of access violations: insufficient privileges (ACLCHECK_NO_PRIV) and ownership requirements (ACLCHECK_NOT_OWNER).

For privilege violations, it generates "permission denied for [object_type] [object_name]" messages. For ownership violations, it generates "must be owner of [object_type] [object_name]" messages. The function supports virtually all PostgreSQL object types including tables, functions, schemas, databases, and many others.

Special handling is provided for certain object types where the error message refers to "relation" instead of the specific object type, as ownership is attached at the relation level. The function will not return for error conditions - it raises an ERROR that terminates the current transaction.

## Parameters / Member Variables
- `aclerr`: The result code from an ACL check (AclResult enum: ACLCHECK_OK, ACLCHECK_NO_PRIV, ACLCHECK_NOT_OWNER)
- `objtype`: The type of database object being checked (ObjectType enum)  
- `objectname`: The name of the specific object for inclusion in error messages

## Dependencies
- Functions called/Symbols referenced:
  - Various OBJECT_* constants (OBJECT_TABLE, OBJECT_FUNCTION, etc.)
  - ACLCHECK_NO_PRIV, ACLCHECK_NOT_OWNER constants
  - gettext_noop (for internationalization)
  - ereport/elog (for error reporting)
  - ERRCODE_INSUFFICIENT_PRIVILEGE
- Called from (representative examples):
  - [restrict_and_check_grant](../r/restrict_and_check_grant.md) (src/backend/catalog/aclchk.c:314)
  - [check_object_ownership](../c/check_object_ownership.md) (src/backend/catalog/objectaddress.c:2399)
  - [ExecCheckPermissions](../E/ExecCheckPermissions.md) (src/backend/executor/execMain.c:618)
  - [DefineRelation](../D/DefineRelation.md) (src/backend/commands/tablecmds.c:841)
  - [CreateFunction](../C/CreateFunction.md) (src/backend/commands/functioncmds.c:1057)
  - [Many other locations throughout the backend]

## Notes and Other Information
- This function never returns for error conditions - it calls ereport(ERROR) which throws an exception
- Object names are not double-quoted in the format strings as many callers provide pre-quoted strings
- Some object types like OBJECT_COLUMN, OBJECT_POLICY refer to "relation" in ownership error messages
- Unsupported object types trigger an elog(ERROR) with "unsupported object type" message
- Error messages are marked with gettext_noop() for internationalization support
- This is a void function that serves purely as an error reporting utility