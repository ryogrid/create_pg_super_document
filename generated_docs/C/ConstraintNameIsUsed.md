# ConstraintNameIsUsed

## Location
[src/backend/catalog/pg_constraint.c:399-443](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_constraint.c#L399-L443)

## Overview
Tests whether a given constraint name is currently in use for a specific object (relation or domain) to determine if a user-specified constraint name is acceptable.

## Definition


## Detailed Description
This function checks if a constraint name is already being used on a specific object (table, index, or domain). Unlike ChooseConstraintName which avoids names used anywhere in the namespace, this function only prevents duplicate constraint names on the same object. It performs a catalog scan of pg_constraint using the appropriate index to efficiently locate any existing constraint with the same name on the specified object. The function is designed to validate user-provided constraint names during DDL operations.

## Parameters / Member Variables
- : Category of constraint - either CONSTRAINT_RELATION for table constraints or CONSTRAINT_DOMAIN for domain constraints
- : OID of the object (relation or domain) to check constraint names against
- : Name of the constraint to check for existence

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [systable_endscan](../s/systable_endscan.md)
  - table_close
  - HeapTupleIsValid
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - [CStringGetDatum](CStringGetDatum.md)
- Called from (representative examples):
  - index_create
  - [RenameConstraintById](../R/RenameConstraintById.md)
  - [ATExecAddConstraint](../A/ATExecAddConstraint.md)
  - [addFkConstraint](../a/addFkConstraint.md)
  - [domainAddCheckConstraint](../d/domainAddCheckConstraint.md)
  - [domainAddNotNullConstraint](../d/domainAddNotNullConstraint.md)

## Notes and Other Information
- Returns true if the constraint name is already used, false otherwise
- Requires exclusive lock on the target object to prevent race conditions with concurrent constraint additions
- Uses ConstraintRelidTypidNameIndexId for efficient scanning
- Only checks for name conflicts on the same object, unlike system-generated name checking
- Part of the constraint name validation process during DDL operations