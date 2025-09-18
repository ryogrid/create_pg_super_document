# RenameType

## Location
src/backend/commands/typecmds.c: 3741 - 3821

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
  - makeTypeNameFromNameList
  - typenameTypeId
  - table_open
  - SearchSysCacheCopy1
  - object_ownercheck
  - aclcheck_error_type
  - get_rel_relkind
  - IsTrueArrayType
  - RenameRelationInternal
  - RenameTypeInternal
  - ObjectAddressSet
- Called from (representative examples):
  - ExecRenameStmt

## Notes and Other Information
- Returns an ObjectAddress pointing to the renamed type for dependency tracking
- Performs ownership validation using object_ownercheck before allowing the rename
- Distinguishes between ALTER TYPE and ALTER DOMAIN commands, preventing misuse
- Prohibits direct renaming of array types, requiring users to rename the base type instead
- Prevents renaming table row types, directing users to use ALTER TABLE instead
- Handles composite types specially by delegating to relation renaming infrastructure
- Uses RowExclusiveLock on the TypeRelationId catalog to prevent concurrent modifications
- Includes comprehensive error reporting with appropriate error codes and hints for alternative approaches