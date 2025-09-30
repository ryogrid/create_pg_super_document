# renameatt_check

## Location
[src/backend/commands/tablecmds.c:3663-3711](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L3663-L3711)

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
  - [errdetail_relkind_not_supported](../e/errdetail_relkind_not_supported.md)
  - [object_ownercheck](../o/object_ownercheck.md)
  - [aclcheck_error](../a/aclcheck_error.md)
  - [get_relkind_objtype](../g/get_relkind_objtype.md)
  - [get_rel_relkind](../g/get_rel_relkind.md)
  - [IsSystemClass](../I/IsSystemClass.md)
  - RELKIND constants (RELATION, VIEW, MATVIEW, etc.)
- Called from (representative examples):
  - [renameatt_internal](renameatt_internal.md)
  - [RangeVarCallbackForRenameAttribute](../R/RangeVarCallbackForRenameAttribute.md)
  - [rename_constraint_internal](rename_constraint_internal.md)

## Notes and Other Information
- Prevents renaming columns of typed tables except during recursive inheritance operations
- Restricts column renaming to specific relation kinds that logically support the operation
- Internal system references use attnum rather than column names, so renaming doesn't break system functionality
- Requires ownership of the relation to perform rename operations
- System catalog modifications are controlled by the allowSystemTableMods setting
- The function is static, indicating it's only used within the tablecmds.c module

## Simplified Source

```c
static void
renameatt_check(Oid myrelid, Form_pg_class classform, bool recursing)
{
    char relkind = classform->relkind;

    // Check if this is a typed table (not allowed unless recursing)
    if (classform->reloftype && !recursing)
        ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("cannot rename column of typed table")));

    // Check if relation kind supports column renaming
    if (relkind != RELKIND_RELATION &&
        relkind != RELKIND_VIEW &&
        relkind != RELKIND_MATVIEW &&
        relkind != RELKIND_COMPOSITE_TYPE &&
        relkind != RELKIND_INDEX &&
        relkind != RELKIND_PARTITIONED_INDEX &&
        relkind != RELKIND_FOREIGN_TABLE &&
        relkind != RELKIND_PARTITIONED_TABLE)
        ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("cannot rename columns of relation \"%s\"",
                        NameStr(classform->relname)),
                 errdetail_relkind_not_supported(relkind)));

    // Check ownership permissions
    if (!object_ownercheck(RelationRelationId, myrelid, GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, get_relkind_objtype(get_rel_relkind(myrelid)),
                       NameStr(classform->relname));

    // Check system catalog modification permissions
    if (!allowSystemTableMods && IsSystemClass(myrelid, classform))
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("permission denied: \"%s\" is a system catalog",
                        NameStr(classform->relname))));
}
```