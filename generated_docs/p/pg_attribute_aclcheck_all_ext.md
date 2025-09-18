# pg_attribute_aclcheck_all_ext

## Location
src/backend/catalog/aclchk.c: 3978 - 4095

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
  - SearchSysCache1, SearchSysCache2
  - HeapTupleIsValid
  - SysCacheGetAttr
  - DatumGetAclP
  - aclmask
  - ACLMASK_ANY, ACLMASK_ALL
  - Form_pg_class, Form_pg_attribute
  - ACLCHECK_NO_PRIV, ERRCODE_UNDEFINED_TABLE
- Called from (representative examples):
  - pg_attribute_aclcheck_all
  - has_any_column_privilege_name_id
  - has_any_column_privilege_id
  - has_any_column_privilege_id_id

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