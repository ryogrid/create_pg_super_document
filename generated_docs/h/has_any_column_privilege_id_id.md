# has_any_column_privilege_id_id

## Location
[src/backend/utils/adt/acl.c:2487-2537](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L2487-L2537)

## Overview
Checks whether a user has any column-level privilege on a table by examining both table-level and individual column-level permissions using role ID, table OID, and privilege name.

## Definition
```c
Datum has_any_column_privilege_id_id(PG_FUNCTION_ARGS)
```

## Detailed Description
This function determines if a specified role has any column privilege on a given table. It takes a two-phase approach: first checking table-level permissions, and if that fails, examining all individual columns to see if the role has the requested privilege on any column. The function is part of PostgreSQL's access control system and is typically invoked through SQL functions for privilege checking.

The function follows PostgreSQL's privilege inheritance model where table-level privileges automatically grant access to all columns, but column-level privileges can also be granted independently.

## Parameters / Member Variables
- `roleid` (OID): The object identifier of the role whose privileges are being checked
- `tableoid` (OID): The object identifier of the table being examined  
- `priv_type_text` (text*): Text string specifying the privilege type (e.g., "SELECT", "INSERT", "UPDATE")

## Dependencies
- Functions called/Symbols referenced:
  - [convert_column_priv_string](../c/convert_column_priv_string.md): Converts privilege text to AclMode
  - [pg_class_aclcheck_ext](../p/pg_class_aclcheck_ext.md): Checks table-level access control permissions
  - [pg_attribute_aclcheck_all_ext](../p/pg_attribute_aclcheck_all_ext.md): Checks column-level access control for all columns
  - [AclResult](../A/AclResult.md): Enumeration type for access control results
  - ACLMASK_ANY: Flag for checking any column privilege
- Called from (representative examples):
  - No direct references found in codebase

## Notes and Other Information
- Returns NULL if the table or role doesn't exist (is_missing flag handling)
- Uses a performance optimization by checking table-level privileges first
- Part of the SQL-callable privilege checking infrastructure
- Located in src/backend/utils/adt/acl.c:2487-2537