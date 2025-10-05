# has_table_privilege_name_name

## Location
[src/backend/utils/adt/acl.c:1895-1920](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L1895-L1920)

## Overview
Checks whether a specific user has a given privilege on a table, taking both the user name and table name as text parameters.

## Definition
```c
Datum has_table_privilege_name_name(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is one of the PostgreSQL has_table_privilege variants that provides access control checking functionality at the SQL level. It determines whether a specified user (identified by name) has a particular privilege on a specified table (identified by name). The function takes three parameters: the role name, table name, and privilege type, all as text/name values.

The function performs its check by:
1. Converting the role name to an OID using get_role_oid_or_public()
2. Converting the table name to an OID using convert_table_name()  
3. Converting the privilege string to an AclMode using convert_table_priv_string()
4. Performing the actual privilege check using pg_class_aclcheck()

## Parameters / Member Variables
- `rolename`: Name type parameter specifying the user whose privileges are being checked
- `tablename`: Text parameter specifying the name of the table to check privileges on
- `priv_type_text`: Text parameter specifying the privilege type (e.g., "SELECT", "INSERT", "UPDATE", "DELETE")

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NAME - to extract Name argument
  - PG_GETARG_TEXT_PP - to extract text arguments  
  - [get_role_oid_or_public](../g/get_role_oid_or_public.md) - converts role name to OID
  - [convert_table_name](../c/convert_table_name.md) - converts table name to OID
  - [convert_table_priv_string](../c/convert_table_priv_string.md) - converts privilege string to AclMode
  - [pg_class_aclcheck](../p/pg_class_aclcheck.md) - performs the actual ACL check
  - PG_RETURN_BOOL - returns boolean result
- Called from (representative examples):
  - SQL has_table_privilege() function calls

## Notes and Other Information
- Returns true if the user has the specified privilege, false otherwise
- All parameters are required and cannot be NULL
- This is one of several overloaded variants of has_table_privilege at the SQL level
- Located in src/backend/utils/adt/acl.c:1895-1920
- Part of PostgreSQL's role-based access control system

## Simplified Source

```c
Datum has_table_privilege_name_name(PG_FUNCTION_ARGS) {
    Name rolename = PG_GETARG_NAME(0);
    text *tablename = PG_GETARG_TEXT_PP(1);
    text *priv_type_text = PG_GETARG_TEXT_PP(2);
    Oid roleid, tableoid;
    AclMode mode;
    AclResult aclresult;

    // Convert names to OIDs and privilege string to mode
    roleid = get_role_oid_or_public(NameStr(*rolename));
    tableoid = convert_table_name(tablename);
    mode = convert_table_priv_string(priv_type_text);

    // Check if user has the specified privilege on the table
    aclresult = pg_class_aclcheck(tableoid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}
```