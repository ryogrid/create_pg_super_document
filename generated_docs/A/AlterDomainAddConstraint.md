# AlterDomainAddConstraint

## Location
src/backend/commands/typecmds.c: 2897 - 3036

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
  - typenameTypeId
  - SearchSysCacheCopy1
  - checkDomainOwner
  - nodeTag
  - domainAddCheckConstraint
  - validateDomainCheckConstraint
  - domainAddNotNullConstraint
  - validateDomainNotNullConstraint
  - CatalogTupleUpdate
  - CacheInvalidateHeapTuple
- Called from (representative examples):
  - ProcessUtilitySlow
  - ATExecCmd

## Notes and Other Information
- Only supports CHECK and NOT NULL constraints for domains
- Provides clear error messages for unsupported constraint types
- Handles validation skipping through the skip_validation flag in constraints
- Updates typnotnull field in pg_type for NOT NULL constraints
- Manually invalidates cache for CHECK constraints since pg_type doesn't change
- Returns early if attempting to add NOT NULL to an already NOT NULL domain