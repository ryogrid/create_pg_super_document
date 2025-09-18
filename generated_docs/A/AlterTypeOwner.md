# AlterTypeOwner

## Location
src/backend/commands/typecmds.c: 3822 - 3946

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
  - makeTypeNameFromNameList
  - LookupTypeName
  - TypeNameToString
  - typeTypeId
  - heap_copytuple
  - ReleaseSysCache
  - get_rel_relkind
  - IsTrueArrayType
  - get_multirange_range
  - superuser
  - object_ownercheck
  - aclcheck_error_type
  - check_can_set_role
  - object_aclcheck
  - aclcheck_error
  - get_namespace_name
  - AlterTypeOwner_oid
  - ObjectAddressSet
- Called from (representative examples):
  - ExecAlterOwnerStmt

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