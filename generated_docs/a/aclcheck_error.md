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

## Simplified Source

```c
// Simplified version of aclcheck_error
void aclcheck_error(AclResult aclerr, ObjectType objtype, const char *objectname) {
    switch (aclerr) {
        case ACLCHECK_OK:
            // No error, so return to caller
            break;

        case ACLCHECK_NO_PRIV:
            {
                const char *msg = get_permission_denied_message(objtype);
                ereport(ERROR,
                        (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                         errmsg(msg, objectname)));
                break;
            }

        case ACLCHECK_NOT_OWNER:
            {
                const char *msg = get_ownership_required_message(objtype);
                ereport(ERROR,
                        (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                         errmsg(msg, objectname)));
                break;
            }

        default:
            elog(ERROR, "unrecognized AclResult: %d", (int) aclerr);
            break;
    }
}

// Helper function for permission denied messages
static const char *get_permission_denied_message(ObjectType objtype) {
    switch (objtype) {
        case OBJECT_TABLE:
            return gettext_noop("permission denied for table %s");
        case OBJECT_FUNCTION:
            return gettext_noop("permission denied for function %s");
        case OBJECT_SCHEMA:
            return gettext_noop("permission denied for schema %s");
        case OBJECT_DATABASE:
            return gettext_noop("permission denied for database %s");
        case OBJECT_VIEW:
            return gettext_noop("permission denied for view %s");
        case OBJECT_INDEX:
            return gettext_noop("permission denied for index %s");
        case OBJECT_SEQUENCE:
            return gettext_noop("permission denied for sequence %s");
        // ... (similar cases for other common object types)
        default:
            if (is_unsupported_type(objtype)) {
                elog(ERROR, "unsupported object type: %d", objtype);
            }
            return gettext_noop("permission denied for object %s");
    }
}

// Helper function for ownership required messages
static const char *get_ownership_required_message(ObjectType objtype) {
    switch (objtype) {
        case OBJECT_TABLE:
            return gettext_noop("must be owner of table %s");
        case OBJECT_FUNCTION:
            return gettext_noop("must be owner of function %s");
        case OBJECT_SCHEMA:
            return gettext_noop("must be owner of schema %s");
        case OBJECT_DATABASE:
            return gettext_noop("must be owner of database %s");
        // Special cases that refer to "relation" for ownership
        case OBJECT_COLUMN:
        case OBJECT_POLICY:
        case OBJECT_RULE:
        case OBJECT_TABCONSTRAINT:
        case OBJECT_TRIGGER:
            return gettext_noop("must be owner of relation %s");
        // ... (similar cases for other common object types)
        default:
            if (is_unsupported_type(objtype)) {
                elog(ERROR, "unsupported object type: %d", objtype);
            }
            return gettext_noop("must be owner of object %s");
    }
}
```

Key simplifications made:
- Extracted repetitive switch cases into helper functions `get_permission_denied_message()` and `get_ownership_required_message()`
- Consolidated the nearly identical object type mappings into more manageable functions
- Preserved the core three-way ACL result handling (OK, NO_PRIV, NOT_OWNER)
- Maintained special handling for relation-based ownership messages
- Kept error handling for unsupported object types
- Focused on the main execution path while abstracting the verbose message mapping logic