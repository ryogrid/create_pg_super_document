# domainAddNotNullConstraint

## Location
src/backend/commands/typecmds.c: 3668 - 3740

## Overview
Internal function that handles the creation and validation of NOT NULL constraints for domain types, shared between CREATE DOMAIN and ALTER DOMAIN operations.

## Definition
```c
static void domainAddNotNullConstraint(Oid domainOid, Oid domainNamespace, Oid baseTypeOid,
                                      int typMod, Constraint *constr,
                                      const char *domainName, ObjectAddress *constrAddr)
```

## Detailed Description
This function implements the core logic for adding NOT NULL constraints to domain types in PostgreSQL. It handles both explicit constraint names and automatic name generation, validates for duplicate constraints, and creates the appropriate catalog entry in pg_constraint. The function is designed to be shared between the CREATE DOMAIN and ALTER DOMAIN commands to ensure consistent behavior.

The function performs constraint name validation or generation, checks for existing constraints with the same name, and creates a new constraint entry in the system catalog. It specifically handles NOT NULL constraints for domains, which are different from table constraints as they apply to the domain type itself rather than individual columns.

## Parameters / Member Variables
- `domainOid`: The OID of the domain type to which the constraint is being added
- `domainNamespace`: The namespace (schema) OID where the domain resides
- `baseTypeOid`: The OID of the underlying base type of the domain
- `typMod`: Type modifier for the domain type
- `constr`: Pointer to the Constraint structure containing constraint details
- `domainName`: String name of the domain for error reporting
- `constrAddr`: Optional output parameter to receive the ObjectAddress of the created constraint

## Dependencies
- Functions called/Symbols referenced:
  - ConstraintNameIsUsed
  - ChooseConstraintName
  - CreateConstraintEntry
  - ObjectAddressSet
  - CONSTR_NOTNULL
  - CONSTRAINT_DOMAIN
  - CONSTRAINT_NOTNULL
- Called from (representative examples):
  - DefineDomain
  - AlterDomainNotNull
  - AlterDomainAddConstraint

## Notes and Other Information
- This is a static function local to typecmds.c, indicating it's an internal implementation detail
- The function asserts that the constraint type is CONSTR_NOTNULL, ensuring type safety
- Automatic constraint naming follows the pattern "not_null" when no explicit name is provided
- The function creates a domain constraint (not a relation constraint) in pg_constraint
- Error handling includes duplicate constraint name detection with appropriate error messages
- The constraint is marked as local, non-inheritable, and non-internal by default