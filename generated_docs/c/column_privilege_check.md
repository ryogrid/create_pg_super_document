# column_privilege_check

## Location
[src/backend/utils/adt/acl.c:2538-2577](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L2538-L2577)

## Overview
A static helper function that checks column privileges without throwing errors for dropped columns or missing tables, returning integer codes to indicate privilege status.

## Definition
```c
static int column_privilege_check(Oid tableoid, AttrNumber attnum, Oid roleid, AclMode mode)
```

## Detailed Description
This internal function performs column privilege checking with graceful error handling. It implements a two-level privilege checking strategy: first checking column-specific privileges, then falling back to table-level privileges. Unlike other privilege checking functions, it returns integer codes rather than throwing exceptions, making it suitable for use in queries that scan system catalogs like pg_attribute.

The function specifically handles cases where columns or tables might be dropped or missing, returning -1 to indicate such conditions rather than raising errors that would interrupt query processing.

## Parameters / Member Variables
- `tableoid` (Oid): The object identifier of the table containing the column
- `attnum` (AttrNumber): The attribute number of the specific column being checked
- `roleid` (Oid): The object identifier of the role whose privileges are being verified
- `mode` (AclMode): The privilege mode being requested (SELECT, INSERT, UPDATE, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_attribute_aclcheck_ext](../p/pg_attribute_aclcheck_ext.md): Checks column-level access control permissions
  - [pg_class_aclcheck_ext](../p/pg_class_aclcheck_ext.md): Checks table-level access control permissions
  - [AclResult](../A/AclResult.md): Enumeration type for access control results
  - InvalidAttrNumber: Constant representing an invalid attribute number
- Called from (representative examples):
  - [has_column_privilege_name_name_name](../h/has_column_privilege_name_name_name.md)
  - [has_column_privilege_name_name_attnum](../h/has_column_privilege_name_name_attnum.md)
  - [has_column_privilege_name_id_name](../h/has_column_privilege_name_id_name.md)
  - [has_column_privilege_id_id_attnum](../h/has_column_privilege_id_id_attnum.md)
  - Multiple other has_column_privilege variant functions

## Notes and Other Information
- Returns 1 for privilege granted, 0 for privilege denied, -1 for dropped/missing objects
- Static function used internally by all has_column_privilege variant functions
- Checks column-level privileges first, then table-level as fallback
- Designed to avoid errors in system catalog scanning operations
- Located in src/backend/utils/adt/acl.c:2538-2577

## Simplified Source

```c
static int
column_privilege_check(Oid tableoid, AttrNumber attnum, Oid roleid, AclMode mode)
{
    AclResult aclresult;
    bool is_missing = false;

    // Check for invalid column number
    if (attnum == InvalidAttrNumber)
        return -1;

    // Check column-level privileges first
    aclresult = pg_attribute_aclcheck_ext(tableoid, attnum, roleid, mode, &is_missing);
    if (aclresult == ACLCHECK_OK)
        return 1;
    else if (is_missing)
        return -1;

    // Fallback to table-level privileges
    aclresult = pg_class_aclcheck_ext(tableoid, roleid, mode, &is_missing);
    if (aclresult == ACLCHECK_OK)
        return 1;
    else if (is_missing)
        return -1;
    else
        return 0;
}
```