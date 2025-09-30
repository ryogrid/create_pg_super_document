# AlterTypeOwner

## Location
[src/backend/commands/typecmds.c:3822-3946](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/typecmds.c#L3822-L3946)

## Overview
Main entry point function that handles ALTER TYPE OWNER and ALTER DOMAIN OWNER commands, performing validation and permission checks before delegating the actual ownership change to internal functions.

## Definition
```c
ObjectAddress AlterTypeOwner(List *names, Oid newOwnerId, ObjectType objecttype)
```

## Detailed Description
AlterTypeOwner is the primary function responsible for executing ALTER TYPE OWNER and ALTER DOMAIN OWNER commands in PostgreSQL. It performs comprehensive validation including type existence checks, object type validation, ownership permissions, and privilege verification for the new owner. The function includes business logic to prevent inappropriate operations on system-managed types like array types, table row types, and multirange types.

The function implements a complete permission model where the current user must own the type, be able to become the new owner (via check_can_set_role), and the new owner must have CREATE privilege in the type's namespace. It handles edge cases like no-op ownership changes for dump restoration and provides appropriate error messages with hints for alternative approaches.

## Parameters / Member Variables
- `names`: List representing the qualified name of the type to change ownership
- `newOwnerId`: OID of the role that will become the new owner of the type
- `objecttype`: ObjectType enum indicating whether this is OBJECT_TYPE or OBJECT_DOMAIN

## Dependencies
- Functions called/Symbols referenced:
  - [makeTypeNameFromNameList](../m/makeTypeNameFromNameList.md)
  - [LookupTypeName](../L/LookupTypeName.md)
  - [TypeNameToString](../T/TypeNameToString.md)
  - [typeTypeId](../t/typeTypeId.md)
  - [heap_copytuple](../h/heap_copytuple.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - [get_rel_relkind](../g/get_rel_relkind.md)
  - IsTrueArrayType
  - [get_multirange_range](../g/get_multirange_range.md)
  - [superuser](../s/superuser.md)
  - [object_ownercheck](../o/object_ownercheck.md)
  - [aclcheck_error_type](../a/aclcheck_error_type.md)
  - [check_can_set_role](../c/check_can_set_role.md)
  - [object_aclcheck](../o/object_aclcheck.md)
  - [aclcheck_error](../a/aclcheck_error.md)
  - [get_namespace_name](../g/get_namespace_name.md)
  - [AlterTypeOwner_oid](AlterTypeOwner_oid.md)
  - ObjectAddressSet
- Called from (representative examples):
  - [ExecAlterOwnerStmt](../E/ExecAlterOwnerStmt.md)

## Notes and Other Information
- Returns an ObjectAddress pointing to the type for dependency tracking
- Uses LookupTypeName instead of typenameTypeId to handle shell types
- Distinguishes between ALTER TYPE and ALTER DOMAIN, preventing command misuse
- Prohibits ownership changes on array types, multirange types, and table row types
- Implements a no-op optimization when the new owner is the same as current owner
- Requires superuser privileges or ownership plus role membership and namespace CREATE privileges
- Uses RowExclusiveLock on TypeRelationId to prevent concurrent modifications
- Provides comprehensive error reporting with hints directing users to appropriate alternative commands
- Delegates the actual ownership change to AlterTypeOwner_oid for implementation

## Simplified Source

```c
ObjectAddress AlterTypeOwner(List *names, Oid newOwnerId, ObjectType objecttype)
{
    // Open type catalog
    Relation rel = table_open(TypeRelationId, RowExclusiveLock);

    // Convert name list to TypeName and look up the type
    TypeName *typename = makeTypeNameFromNameList(names);
    HeapTuple tup = LookupTypeName(NULL, typename, NULL, false);

    if (!tup)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("type \"%s\" does not exist",
                       TypeNameToString(typename))));

    Oid typeOid = typeTypeId(tup);

    // Get a modifiable copy of the type tuple
    HeapTuple newtup = heap_copytuple(tup);
    ReleaseSysCache(tup);
    Form_pg_type typTup = (Form_pg_type) GETSTRUCT(newtup);

    // Validate object type constraints
    validate_type_ownership_constraints(typTup, typeOid, objecttype);

    // Check permissions and change ownership if needed
    if (typTup->typowner != newOwnerId) {
        if (!superuser()) {
            check_type_ownership_permissions(typTup, newOwnerId);
        }
        AlterTypeOwner_oid(typeOid, newOwnerId, true);
    }

    // Build return address
    ObjectAddress address;
    ObjectAddressSet(address, TypeRelationId, typeOid);

    table_close(rel, RowExclusiveLock);
    return address;
}

static void validate_type_ownership_constraints(Form_pg_type typTup,
                                                Oid typeOid,
                                                ObjectType objecttype)
{
    // Prevent ALTER DOMAIN on non-domain types
    if (objecttype == OBJECT_DOMAIN && typTup->typtype != TYPTYPE_DOMAIN)
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("%s is not a domain", format_type_be(typeOid))));

    // Prevent ownership changes on table row types
    if (typTup->typtype == TYPTYPE_COMPOSITE &&
        get_rel_relkind(typTup->typrelid) != RELKIND_COMPOSITE_TYPE)
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("%s is a table's row type", format_type_be(typeOid)),
                errhint("Use ALTER TABLE instead.")));

    // Prevent direct changes on array and multirange types
    if (IsTrueArrayType(typTup) || typTup->typtype == TYPTYPE_MULTIRANGE)
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("cannot alter %s type directly",
                       typTup->typtype == TYPTYPE_MULTIRANGE ? "multirange" : "array")));
}
```