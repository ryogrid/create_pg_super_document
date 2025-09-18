# AlterDomainAddConstraint

## Location
[src/backend/commands/typecmds.c:2897-3036](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/typecmds.c#L2897-L3036)

## Overview
Implements the ALTER DOMAIN ADD CONSTRAINT statement, adding CHECK or NOT NULL constraints to domain types with proper validation and constraint enforcement.

## Definition


## Detailed Description
This function adds constraints to domain types, supporting CHECK and NOT NULL constraint types while explicitly rejecting unsupported constraint types like UNIQUE, PRIMARY KEY, FOREIGN KEY, and EXCLUSION. For CHECK constraints, it processes the constraint expression, adds an entry to pg_constraint, and optionally validates existing data. For NOT NULL constraints, it sets the typnotnull flag and validates existing data unless validation is skipped. The function ensures proper cache invalidation for constraint changes that don't modify the pg_type row directly.

## Parameters / Member Variables
- : List of qualified names identifying the domain to modify
- : Node representing the constraint to add (must be a Constraint node)
- : Output parameter receiving the ObjectAddress of the created constraint

## Dependencies
- Functions called/Symbols referenced:
  - makeTypeNameFromNameList
  - [typenameTypeId](../t/typenameTypeId.md)
  - SearchSysCacheCopy1
  - [checkDomainOwner](../c/checkDomainOwner.md)
  - nodeTag
  - [domainAddCheckConstraint](../d/domainAddCheckConstraint.md)
  - [validateDomainCheckConstraint](../v/validateDomainCheckConstraint.md)
  - [domainAddNotNullConstraint](../d/domainAddNotNullConstraint.md)
  - [validateDomainNotNullConstraint](../v/validateDomainNotNullConstraint.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [CacheInvalidateHeapTuple](../C/CacheInvalidateHeapTuple.md)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)
  - [ATExecCmd](ATExecCmd.md)

## Notes and Other Information
- Only supports CHECK and NOT NULL constraints for domains
- Provides clear error messages for unsupported constraint types
- Handles validation skipping through the skip_validation flag in constraints
- Updates typnotnull field in pg_type for NOT NULL constraints
- Manually invalidates cache for CHECK constraints since pg_type doesn't change
- Returns early if attempting to add NOT NULL to an already NOT NULL domain