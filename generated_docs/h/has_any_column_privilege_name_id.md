# has_any_column_privilege_name_id

## Location
[src/backend/utils/adt/acl.c:2392-2426](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L2392-L2426)

## Overview
Checks if a specified user (by name) has any given privilege on any column of a specified table (by OID), with NULL handling for non-existent objects.

## Definition

```c
Datum
has_any_column_privilege_name_id(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is a hybrid variant that takes a user name and table OID as inputs, providing more precise table identification while maintaining user-friendly name-based user specification. A key feature of this function is its robust error handling: it returns NULL if either the table OID doesn't correspond to an existing table or if other database objects are missing, rather than throwing errors.

The function uses the '_ext' variants of the privilege checking functions ( and ) which provide additional error information through the  parameter. This allows the function to distinguish between permission denial and missing objects, returning NULL for the latter case.

Like other variants in this family, it performs hierarchical checking: table-level privileges first, then column-level privileges if the table-level check fails.

## Parameters / Member Variables
-  (username): The name of the role/user to check privileges for
-  (tableoid): The OID of the table to check column privileges on
-  (priv_type_text): The privilege type to check (e.g., 'SELECT', 'INSERT', 'UPDATE')

## Dependencies
- Functions called/Symbols referenced:
  - : Converts role name to OID, handling 'public' role specially
  - : Converts privilege string to AclMode bitmask
  - : Checks table-level privileges with missing object detection
  - : Checks column-level privileges with missing object detection
  - : Constant used for checking if any column has the privilege
- Called from (representative examples):
  - SQL queries via function call interface (no direct C callers found)

## Notes and Other Information
- Returns boolean for valid objects, NULL if the table OID doesn't exist or objects are missing
- Uses extended ('_ext') versions of privilege checking functions for better error handling
- The table OID parameter allows for more precise and efficient table identification compared to name-based lookups
- Part of the overloaded 'has_any_column_privilege' family of functions at the SQL level
- The NULL return behavior makes it safer for use in SQL queries that might reference non-existent tables
- Located in src/backend/utils/adt/acl.c:2392-2426

## Simplified Source

```c
Datum
has_any_column_privilege_name_id(PG_FUNCTION_ARGS)
{
    Name username = PG_GETARG_NAME(0);
    Oid  tableoid = PG_GETARG_OID(1);
    text *priv_type_text = PG_GETARG_TEXT_PP(2);
    bool is_missing = false;

    // Convert username to role OID
    Oid roleid = get_role_oid_or_public(NameStr(*username));

    // Parse privilege string to internal mode
    AclMode mode = convert_column_priv_string(priv_type_text);

    // Check table-level privileges first with missing object detection
    AclResult aclresult = pg_class_aclcheck_ext(tableoid, roleid, mode, &is_missing);

    // If table-level fails, check column-level privileges
    if (aclresult != ACLCHECK_OK) {
        if (is_missing)
            PG_RETURN_NULL();
        aclresult = pg_attribute_aclcheck_all_ext(tableoid, roleid, mode, ACLMASK_ANY, &is_missing);
        if (is_missing)
            PG_RETURN_NULL();
    }

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}
```