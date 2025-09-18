# AlterDomainValidateConstraint

## Location
src/backend/commands/typecmds.c: 3037 - 3135

## Overview
Implements the ALTER DOMAIN VALIDATE CONSTRAINT statement, validating an existing check constraint on a domain and marking it as validated in the catalog.

## Definition


## Detailed Description
This function validates an existing check constraint on a domain type by first locating the constraint in pg_constraint, verifying it's a check constraint, extracting the constraint expression, and running validation against all existing data. After successful validation, it updates the constraint's convalidated flag to true in the catalog. The function ensures proper constraint validation semantics while maintaining catalog consistency and triggering appropriate hooks for change notification.

## Parameters / Member Variables
- : List of qualified names identifying the domain containing the constraint
- : Name of the check constraint to validate

## Dependencies
- Functions called/Symbols referenced:
  - makeTypeNameFromNameList
  - typenameTypeId
  - SearchSysCache1
  - checkDomainOwner
  - ScanKeyInit
  - systable_beginscan
  - systable_getnext
  - systable_endscan
  - SysCacheGetAttrNotNull
  - TextDatumGetCString
  - validateDomainCheckConstraint
  - heap_copytuple
  - heap_freetuple
  - CatalogTupleUpdate
  - InvokeObjectPostAlterHook
  - TypeNameToString
  - ReleaseSysCache
- Called from (representative examples):
  - ProcessUtilitySlow

## Notes and Other Information
- Only works with check constraints, rejects other constraint types with appropriate error messages
- Uses a three-key scan to efficiently locate the target constraint in pg_constraint
- Validates all existing domain values against the constraint expression before marking as validated
- Updates the convalidated flag in a copied tuple to ensure proper catalog semantics
- Properly manages system cache and relation locks throughout the operation
- Triggers post-alter hooks for proper event notification in the constraint system