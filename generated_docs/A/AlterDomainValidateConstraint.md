# AlterDomainValidateConstraint

## Location
[src/backend/commands/typecmds.c:3037-3135](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/typecmds.c#L3037-L3135)

## Overview
Implements the ALTER DOMAIN VALIDATE CONSTRAINT statement, validating an existing check constraint on a domain and marking it as validated in the catalog.

## Definition

```c
ObjectAddress
AlterDomainValidateConstraint(List *names, const char *constrName)
```
## Detailed Description
This function validates an existing check constraint on a domain type by first locating the constraint in pg_constraint, verifying it's a check constraint, extracting the constraint expression, and running validation against all existing data. After successful validation, it updates the constraint's convalidated flag to true in the catalog. The function ensures proper constraint validation semantics while maintaining catalog consistency and triggering appropriate hooks for change notification.

## Parameters / Member Variables
- : List of qualified names identifying the domain containing the constraint
- : Name of the check constraint to validate

## Dependencies
- Functions called/Symbols referenced:
  - [makeTypeNameFromNameList](../m/makeTypeNameFromNameList.md)
  - [typenameTypeId](../t/typenameTypeId.md)
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [checkDomainOwner](../c/checkDomainOwner.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [systable_endscan](../s/systable_endscan.md)
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md)
  - TextDatumGetCString
  - [validateDomainCheckConstraint](../v/validateDomainCheckConstraint.md)
  - [heap_copytuple](../h/heap_copytuple.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - InvokeObjectPostAlterHook
  - [TypeNameToString](../T/TypeNameToString.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
- Only works with check constraints, rejects other constraint types with appropriate error messages
- Uses a three-key scan to efficiently locate the target constraint in pg_constraint
- Validates all existing domain values against the constraint expression before marking as validated
- Updates the convalidated flag in a copied tuple to ensure proper catalog semantics
- Properly manages system cache and relation locks throughout the operation
- Triggers post-alter hooks for proper event notification in the constraint system