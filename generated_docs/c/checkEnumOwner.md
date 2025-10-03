# checkEnumOwner

## Location
[src/backend/commands/typecmds.c:1319-1345](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/typecmds.c#L1319-L1345)

## Overview
checkEnumOwner is a static validation function that verifies a PostgreSQL type is an enum and that the current user has permission to perform ALTER TYPE operations on it.

## Definition

```c
enum */
	if (typTup->typtype != TYPTYPE_ENUM)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("%s is not an enum",
						format_type_be(typTup->oid))));
```
## Detailed Description
This function performs two critical validations before allowing enum-related operations:
1. **Type validation**: Ensures the type referenced by the heap tuple is actually an enum type (TYPTYPE_ENUM)
2. **Permission validation**: Verifies the current user owns the enum type and has ALTER TYPE privileges

The function operates on a heap tuple containing type information from the pg_type system catalog. If either validation fails, it throws an appropriate error with specific error codes and messages.

## Parameters / Member Variables
- `TYPTYPE_ENUM`: HeapTuple containing the type information from pg_type catalog that needs to be validated as an enum with proper ownership
## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_type (macro for accessing pg_type tuple structure)
  - TYPTYPE_ENUM (constant defining enum type classification)
  - [object_ownercheck](../o/object_ownercheck.md) (checks object ownership permissions)
  - [aclcheck_error_type](../a/aclcheck_error_type.md) (reports access control errors for types)
  - ACLCHECK_NOT_OWNER (access control error code constant)
- Called from (representative examples):
  - AlterTypeRecurseParams
  - [AlterEnum](../A/AlterEnum.md)

## Notes and Other Information
- This is a static function internal to typecmds.c, used as a helper for enum modification operations
- Throws ERRCODE_WRONG_OBJECT_TYPE if the type is not an enum
- Throws access control errors if the user lacks ownership privileges
- Essential security check preventing unauthorized enum modifications

## Simplified Source

```c
static void
checkEnumOwner(HeapTuple tup)
{
    Form_pg_type typTup = (Form_pg_type) GETSTRUCT(tup);

    // Verify this is actually an enum type
    if (typTup->typtype != TYPTYPE_ENUM)
        ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("%s is not an enum", format_type_be(typTup->oid))));

    // Check that current user owns this enum type
    if (!object_ownercheck(TypeRelationId, typTup->oid, GetUserId()))
        aclcheck_error_type(ACLCHECK_NOT_OWNER, typTup->oid);
}
```