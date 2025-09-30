# RenameType

## Location
[src/backend/commands/typecmds.c:3741-3821](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/typecmds.c#L3741-L3821)

## Overview
Main entry point function that handles the execution of ALTER TYPE RENAME commands, performing validation and delegating to appropriate internal renaming functions based on the type being renamed.

## Definition
```c
ObjectAddress RenameType(RenameStmt *stmt)
```

## Detailed Description
RenameType is the primary function responsible for executing ALTER TYPE RENAME and ALTER DOMAIN RENAME commands in PostgreSQL. It performs comprehensive validation including ownership checks, type category validation, and special handling for different type categories (domains, composite types, array types, etc.).

The function first resolves the type name and validates permissions, then applies specific business rules based on the type category. For composite types, it delegates to RenameRelationInternal since composite types have associated pg_class entries. For other types, it uses RenameTypeInternal. The function includes important safety checks to prevent inappropriate operations like renaming array types directly or using ALTER DOMAIN on non-domain types.

## Parameters / Member Variables
- `stmt`: Pointer to RenameStmt structure containing the rename command details including the type to rename and the new name

## Dependencies
- Functions called/Symbols referenced:
  - [makeTypeNameFromNameList](../m/makeTypeNameFromNameList.md)
  - [typenameTypeId](../t/typenameTypeId.md)
  - [table_open](../t/table_open.md)
  - SearchSysCacheCopy1
  - [object_ownercheck](../o/object_ownercheck.md)
  - [aclcheck_error_type](../a/aclcheck_error_type.md)
  - [get_rel_relkind](../g/get_rel_relkind.md)
  - IsTrueArrayType
  - [RenameRelationInternal](RenameRelationInternal.md)
  - [RenameTypeInternal](RenameTypeInternal.md)
  - ObjectAddressSet
- Called from (representative examples):
  - [ExecRenameStmt](../E/ExecRenameStmt.md)

## Notes and Other Information
- Returns an ObjectAddress pointing to the renamed type for dependency tracking
- Performs ownership validation using object_ownercheck before allowing the rename
- Distinguishes between ALTER TYPE and ALTER DOMAIN commands, preventing misuse
- Prohibits direct renaming of array types, requiring users to rename the base type instead
- Prevents renaming table row types, directing users to use ALTER TABLE instead
- Handles composite types specially by delegating to relation renaming infrastructure
- Uses RowExclusiveLock on the TypeRelationId catalog to prevent concurrent modifications
- Includes comprehensive error reporting with appropriate error codes and hints for alternative approaches

## Simplified Source

```c
ObjectAddress
RenameType(RenameStmt *stmt)
{
    List *names = castNode(List, stmt->object);
    const char *newTypeName = stmt->newname;
    TypeName *typename;
    Oid typeOid;
    Relation rel;
    HeapTuple tup;
    Form_pg_type typTup;
    ObjectAddress address;

    // Resolve the type name to get its OID
    typename = makeTypeNameFromNameList(names);
    typeOid = typenameTypeId(NULL, typename);

    // Open type catalog and look up the type
    rel = table_open(TypeRelationId, RowExclusiveLock);
    tup = SearchSysCacheCopy1(TYPEOID, ObjectIdGetDatum(typeOid));
    if (!HeapTupleIsValid(tup))
        elog(ERROR, "cache lookup failed for type %u", typeOid);

    typTup = (Form_pg_type) GETSTRUCT(tup);

    // Check ownership permission
    if (!object_ownercheck(TypeRelationId, typeOid, GetUserId()))
        aclcheck_error_type(ACLCHECK_NOT_OWNER, typeOid);

    // Validate ALTER DOMAIN used only on domain types
    if (stmt->renameType == OBJECT_DOMAIN && typTup->typtype != TYPTYPE_DOMAIN)
        ereport(ERROR, "object is not a domain");

    // Check composite types - must be standalone, not table row types
    if (typTup->typtype == TYPTYPE_COMPOSITE &&
        get_rel_relkind(typTup->typrelid) != RELKIND_COMPOSITE_TYPE)
        ereport(ERROR, "cannot rename table row type, use ALTER TABLE");

    // Prevent direct alteration of array types
    if (IsTrueArrayType(typTup))
        ereport(ERROR, "cannot alter array type directly, alter base type instead");

    // Perform the actual rename
    if (typTup->typtype == TYPTYPE_COMPOSITE) {
        // Composite types need relation renaming too
        RenameRelationInternal(typTup->typrelid, newTypeName, false, false);
    } else {
        // Regular types use type-specific renaming
        RenameTypeInternal(typeOid, newTypeName, typTup->typnamespace);
    }

    // Cleanup and return
    ObjectAddressSet(address, TypeRelationId, typeOid);
    table_close(rel, RowExclusiveLock);

    return address;
}
```