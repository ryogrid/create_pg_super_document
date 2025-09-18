# AlterTypeOwner_oid

## Location
[src/backend/commands/typecmds.c:3947-3986](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/typecmds.c#L3947-L3986)

## Overview
Low-level function that unconditionally changes the ownership of a type by OID, handling dependent types and composite type special cases without performing permission checks.

## Definition
```c
void AlterTypeOwner_oid(Oid typeOid, Oid newOwnerId, bool hasDependEntry)
```

## Detailed Description
AlterTypeOwner_oid is the internal implementation function that performs the actual ownership change operation after all permission checks have been completed. This function is designed to be called by higher-level functions like AlterTypeOwner and by system operations like REASSIGN OWNED BY that have already validated the operation.

The function handles the complexity of composite types by delegating to ATExecChangeOwner, which manages both the pg_class and pg_type entries appropriately. For non-composite types, it calls AlterTypeOwnerInternal directly. The function also manages shared dependency entries and invokes post-alter hooks to ensure proper system consistency and extensibility support.

## Parameters / Member Variables
- `typeOid`: The OID of the type whose ownership is being changed
- `newOwnerId`: The OID of the role that will become the new owner
- `hasDependEntry`: Boolean flag indicating whether to update the pg_shdepend entry (false for table rowtypes and dependent types)

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [ATExecChangeOwner](ATExecChangeOwner.md)
  - [AlterTypeOwnerInternal](AlterTypeOwnerInternal.md)
  - [changeDependencyOnOwner](../c/changeDependencyOnOwner.md)
  - InvokeObjectPostAlterHook
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - table_close
  - TYPTYPE_COMPOSITE
  - AccessExclusiveLock
- Called from (representative examples):
  - [AlterTypeOwner](AlterTypeOwner.md)
  - [shdepReassignOwned_Owner](../s/shdepReassignOwned_Owner.md)

## Notes and Other Information
- This is a void function that assumes all validation has been performed by the caller
- Uses RowExclusiveLock on TypeRelationId for the duration of the operation
- Handles composite types specially by delegating to table infrastructure (ATExecChangeOwner)
- Updates shared dependency records only when hasDependEntry is true
- Invokes post-alter hooks to support extensions and triggers
- Used by both user commands (ALTER TYPE OWNER) and system operations (REASSIGN OWNED BY)
- The hasDependEntry parameter allows selective dependency management for different contexts
- Recursive handling is mentioned in comments but implemented through the called functions