# pg_attribute_aclmask_ext

## Location
src/backend/catalog/aclchk.c: 3215 - 3328

## Overview
The comprehensive implementation for examining user privileges on table columns, including robust handling of missing columns, dropped columns, and concurrent relation drops.

## Definition
```c
static AclMode pg_attribute_aclmask_ext(Oid table_oid, AttrNumber attnum, Oid roleid, AclMode mask, AclMaskHow how, bool *is_missing)
```

## Detailed Description
The `pg_attribute_aclmask_ext` function provides the complete implementation for PostgreSQL's column-level access control checking. It performs a comprehensive multi-step process to validate column existence, handle dropped columns, retrieve column-specific ACLs, and evaluate permissions.

The function first searches for the specific column in pg_attribute using both the table OID and attribute number. It includes special handling for dropped columns, treating them as non-existent when appropriate. Unlike table-level ACL functions, this function includes an important optimization: it hard-codes the knowledge that the default ACL for a column grants no privileges, allowing it to return quickly when no explicit column ACL exists (attacl is null).

To properly evaluate ACLs, the function must also retrieve the table's owner from pg_class, as the column ACL is evaluated against the table owner's rights. The function includes robust error handling for concurrent relation drops and provides optional graceful handling of missing objects via the `is_missing` parameter.

The function performs proper memory management for potentially large ACL objects by handling detoasting and cleanup, and manages system cache interactions carefully with proper SearchSysCache/ReleaseSysCache pairs.

## Parameters / Member Variables
- `table_oid`: The OID of the table containing the column
- `attnum`: The attribute number of the specific column being checked
- `roleid`: The OID of the role whose column privileges are being examined
- `mask`: The access permissions being requested (AclMode bitmask)
- `how`: Specifies the method for ACL evaluation (AclMaskHow enum)
- `is_missing`: Optional output parameter; if not NULL, set to true if column/table doesn't exist or is dropped

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache2
  - SysCacheGetAttr
  - DatumGetAclP
  - aclmask
  - ObjectIdGetDatum
  - Int16GetDatum
  - ReleaseSysCache
  - pfree
  - GETSTRUCT
  - HeapTupleIsValid
  - Form_pg_class
  - Form_pg_attribute
- Called from (representative examples):
  - InternalDefaultACL
  - pg_attribute_aclmask
  - pg_attribute_aclcheck_ext

## Notes and Other Information
- This is a static function internal to the aclchk.c module
- Includes special handling for dropped columns (attisdropped check)
- Optimized for the common case where no explicit column ACL exists (returns 0 immediately)
- Handles concurrent relation drops gracefully when `is_missing` parameter is provided
- Uses ATTNUM system cache for efficient column lookups
- Retrieves table owner from pg_class since column ACLs are evaluated against table ownership
- Performs proper ACL detoasting and memory cleanup
- Does not include superuser bypass logic - this must be handled by callers
- Provides detailed error messages for different failure scenarios (undefined column, undefined table)