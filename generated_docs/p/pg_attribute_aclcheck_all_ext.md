# pg_attribute_aclcheck_all_ext

## Location
[src/backend/catalog/aclchk.c:3978-4095](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L3978-L4095)

## Overview
This function checks a user's access privileges to any or all columns in a table with extended support for detecting missing relations.

## Definition
```c
AclResult pg_attribute_aclcheck_all_ext(Oid table_oid, Oid roleid, AclMode mode, AclMaskHow how, bool *is_missing)
```

## Detailed Description
The `pg_attribute_aclcheck_all_ext` function is an extended version of column-level privilege checking that verifies whether a specified user (role) has the requested access privileges to columns within a table. It supports two checking modes: ACLMASK_ANY (user must have privileges on at least one non-dropped column) and ACLMASK_ALL (user must have privileges on all non-dropped columns). The function iterates through all columns in the relation, checking each one's ACL against the requested privileges. It provides enhanced error handling by distinguishing between missing relations and permission failures through the is_missing parameter.

## Parameters / Member Variables
- `table_oid`: The OID of the table containing the columns to be checked
- `roleid`: The OID of the role (user) whose privileges are being checked
- `mode`: The access mode being requested (e.g., ACL_SELECT, ACL_UPDATE)
- `how`: Specifies checking mode - ACLMASK_ANY (any column) or ACLMASK_ALL (all columns)
- `is_missing`: Output parameter that indicates whether the relation was found to be missing

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md), SearchSysCache2
  - HeapTupleIsValid
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - DatumGetAclP
  - [aclmask](../a/aclmask.md)
  - ACLMASK_ANY, ACLMASK_ALL
  - Form_pg_class, Form_pg_attribute
  - ACLCHECK_NO_PRIV, ERRCODE_UNDEFINED_TABLE
- Called from (representative examples):
  - [pg_attribute_aclcheck_all](pg_attribute_aclcheck_all.md)
  - [has_any_column_privilege_name_id](../h/has_any_column_privilege_name_id.md)
  - [has_any_column_privilege_id](../h/has_any_column_privilege_id.md)
  - [has_any_column_privilege_id_id](../h/has_any_column_privilege_id_id.md)

## Notes and Other Information
- This is the core implementation for checking column privileges across multiple columns
- Fetches relation metadata from pg_class to get owner and column count
- Iterates through all columns, skipping dropped columns
- Uses hard-wired knowledge that default column ACL grants no privileges for optimization
- For ACLMASK_ANY: succeeds on first column with required privileges
- For ACLMASK_ALL: fails on first column without required privileges
- Handles missing relations gracefully when is_missing parameter is provided
- Returns ACLCHECK_NO_PRIV if no non-dropped columns exist
- Located in src/backend/catalog/aclchk.c lines 3978-4095

## Simplified Source

```c
AclResult pg_attribute_aclcheck_all_ext(Oid table_oid, Oid roleid,
                                        AclMode mode, AclMaskHow how,
                                        bool *is_missing) {
    AclResult result;
    HeapTuple classTuple;
    Form_pg_class classForm;
    Oid ownerId;
    AttrNumber nattrs;
    AttrNumber curr_att;

    // Look up table to get owner and number of attributes
    classTuple = SearchSysCache1(RELOID, ObjectIdGetDatum(table_oid));
    if (!HeapTupleIsValid(classTuple)) {
        if (is_missing != NULL) {
            *is_missing = true;
            return ACLCHECK_NO_PRIV;
        } else
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE),
                           errmsg("relation with OID %u does not exist", table_oid)));
    }

    classForm = (Form_pg_class) GETSTRUCT(classTuple);
    ownerId = classForm->relowner;
    nattrs = classForm->relnatts;
    ReleaseSysCache(classTuple);

    // Initialize result for case of no non-dropped columns
    result = ACLCHECK_NO_PRIV;

    // Check privileges on each non-dropped column
    for (curr_att = 1; curr_att <= nattrs; curr_att++) {
        HeapTuple attTuple;
        Datum aclDatum;
        bool isNull;
        Acl *acl;
        AclMode attmask;

        // Look up column metadata
        attTuple = SearchSysCache2(ATTNUM, ObjectIdGetDatum(table_oid),
                                   Int16GetDatum(curr_att));

        if (!HeapTupleIsValid(attTuple))
            continue;

        // Skip dropped columns
        if (((Form_pg_attribute) GETSTRUCT(attTuple))->attisdropped) {
            ReleaseSysCache(attTuple);
            continue;
        }

        // Get column ACL
        aclDatum = SysCacheGetAttr(ATTNUM, attTuple, Anum_pg_attribute_attacl, &isNull);

        // Check privileges (default ACL grants no privileges)
        if (isNull)
            attmask = 0;
        else {
            acl = DatumGetAclP(aclDatum);
            attmask = aclmask(acl, roleid, ownerId, mode, ACLMASK_ANY);
            if ((Pointer) acl != DatumGetPointer(aclDatum))
                pfree(acl);
        }

        ReleaseSysCache(attTuple);

        // Apply checking logic based on how parameter
        if (attmask != 0) {
            result = ACLCHECK_OK;
            if (how == ACLMASK_ANY)
                break;  // Success on any column with privileges
        } else {
            result = ACLCHECK_NO_PRIV;
            if (how == ACLMASK_ALL)
                break;  // Failure on any column without privileges
        }
    }

    return result;
}
```