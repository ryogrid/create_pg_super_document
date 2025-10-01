# aclcheck_error_col

## Location
[src/backend/catalog/aclchk.c:2994-3023](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L2994-L3023)

## Overview
Specialized error reporting function for column-level access control check failures, providing detailed error messages that include both the column name and relation name.

## Definition
```c
void aclcheck_error_col(AclResult aclerr, ObjectType objtype, const char *objectname, const char *colname)
```

## Detailed Description
This function extends the basic aclcheck_error functionality to handle column-specific access control violations. It provides more detailed error messages that specify both the column name and the relation name when permission checks fail at the column level.

For privilege violations (ACLCHECK_NO_PRIV), it generates the specific message "permission denied for column \"[colname]\" of relation \"[objectname]\"". For ownership violations (ACLCHECK_NOT_OWNER), it delegates to the standard aclcheck_error function since columns don't have separate owners - ownership is always at the relation level.

This function is primarily used when checking column-level privileges such as SELECT or UPDATE permissions on specific columns of a table or view.

## Parameters / Member Variables
- `aclerr`: The result code from an ACL check (AclResult enum: ACLCHECK_OK, ACLCHECK_NO_PRIV, ACLCHECK_NOT_OWNER)
- `objtype`: The type of database object (typically OBJECT_TABLE, OBJECT_VIEW, etc.)
- `objectname`: The name of the relation containing the column
- `colname`: The name of the specific column for which access was denied

## Dependencies
- Functions called/Symbols referenced:
  - ACLCHECK_NO_PRIV, ACLCHECK_NOT_OWNER constants
  - [aclcheck_error](aclcheck_error.md) (for ownership violation handling)
  - ereport (for error reporting)
  - elog (for unexpected error conditions)
  - ERRCODE_INSUFFICIENT_PRIVILEGE
- Called from:
  - [restrict_and_check_grant](../r/restrict_and_check_grant.md) (src/backend/catalog/aclchk.c:312)

## Notes and Other Information
- This function never returns for error conditions - it calls ereport(ERROR) which throws an exception
- Column names and object names are double-quoted in the error message for clarity
- Ownership violations are handled by delegating to aclcheck_error since columns inherit ownership from their parent relation
- The function assumes the objtype represents a relation-like object that can contain columns
- This is a specialized variant of aclcheck_error designed specifically for column-level permission failures
- Provides more granular error reporting than the general aclcheck_error function

## Simplified Source

```c
void aclcheck_error_col(AclResult aclerr, ObjectType objtype,
                       const char *objectname, const char *colname) {
    switch (aclerr) {
        case ACLCHECK_OK:
            // No error, return normally
            break;

        case ACLCHECK_NO_PRIV:
            // Report column-specific permission denied error
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                           errmsg("permission denied for column \"%s\" of relation \"%s\"",
                                  colname, objectname)));
            break;

        case ACLCHECK_NOT_OWNER:
            // Delegate to standard error handler (columns inherit ownership)
            aclcheck_error(aclerr, objtype, objectname);
            break;

        default:
            // Handle unexpected error codes
            elog(ERROR, "unrecognized AclResult: %d", (int) aclerr);
            break;
    }
}
```