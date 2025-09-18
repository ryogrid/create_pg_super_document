# renameatt_check

## Location
src/backend/commands/tablecmds.c: 3663 - 3711

## Overview
renameatt_check performs comprehensive validation before allowing an attribute (column) rename operation, checking relation types, ownership permissions, and system catalog restrictions.

## Definition
```c
static void renameatt_check(Oid myrelid, Form_pg_class classform, bool recursing)
```

## Detailed Description
This static function serves as a gatekeeper for attribute rename operations by performing essential sanity checks. It validates that the relation type supports column renaming (tables, views, materialized views, composite types, indexes, and foreign tables), ensures the user has appropriate ownership permissions, prevents renaming columns of typed tables (unless during recursion), and blocks modifications to system catalogs when not explicitly allowed. The function raises errors for invalid operations rather than returning status codes.

## Parameters / Member Variables
- `myrelid`: OID of the relation whose attribute is being renamed
- `classform`: The pg_class tuple form containing relation metadata
- `recursing`: Boolean indicating if this is a recursive call (allows typed table column renames during inheritance processing)

## Dependencies
- Functions called/Symbols referenced:
  - errdetail_relkind_not_supported
  - object_ownercheck
  - aclcheck_error
  - get_relkind_objtype
  - get_rel_relkind
  - IsSystemClass
  - RELKIND constants (RELATION, VIEW, MATVIEW, etc.)
- Called from (representative examples):
  - renameatt_internal
  - RangeVarCallbackForRenameAttribute
  - rename_constraint_internal

## Notes and Other Information
- Prevents renaming columns of typed tables except during recursive inheritance operations
- Restricts column renaming to specific relation kinds that logically support the operation
- Internal system references use attnum rather than column names, so renaming doesn't break system functionality
- Requires ownership of the relation to perform rename operations
- System catalog modifications are controlled by the allowSystemTableMods setting
- The function is static, indicating it's only used within the tablecmds.c module