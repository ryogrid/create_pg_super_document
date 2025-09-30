# domainAddNotNullConstraint

## Location
[src/backend/commands/typecmds.c:3668-3740](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/typecmds.c#L3668-L3740)

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
  - [ConstraintNameIsUsed](../C/ConstraintNameIsUsed.md)
  - [ChooseConstraintName](../C/ChooseConstraintName.md)
  - [CreateConstraintEntry](../C/CreateConstraintEntry.md)
  - ObjectAddressSet
  - CONSTR_NOTNULL
  - CONSTRAINT_DOMAIN
  - CONSTRAINT_NOTNULL
- Called from (representative examples):
  - [DefineDomain](../D/DefineDomain.md)
  - [AlterDomainNotNull](../A/AlterDomainNotNull.md)
  - [AlterDomainAddConstraint](../A/AlterDomainAddConstraint.md)

## Notes and Other Information
- This is a static function local to typecmds.c, indicating it's an internal implementation detail
- The function asserts that the constraint type is CONSTR_NOTNULL, ensuring type safety
- Automatic constraint naming follows the pattern "not_null" when no explicit name is provided
- The function creates a domain constraint (not a relation constraint) in pg_constraint
- Error handling includes duplicate constraint name detection with appropriate error messages
- The constraint is marked as local, non-inheritable, and non-internal by default

## Simplified Source

```c
static void
domainAddNotNullConstraint(Oid domainOid, Oid domainNamespace, Oid baseTypeOid,
                          int typMod, Constraint *constr,
                          const char *domainName, ObjectAddress *constrAddr)
{
    Oid constraint_oid;

    Assert(constr->contype == CONSTR_NOTNULL);

    // Generate or validate constraint name
    if (constr->conname)
    {
        if (ConstraintNameIsUsed(CONSTRAINT_DOMAIN, domainOid, constr->conname))
            ereport(ERROR, "constraint already exists for domain");
    }
    else
    {
        constr->conname = ChooseConstraintName(domainName, NULL, "not_null",
                                              domainNamespace, NIL);
    }

    // Create constraint entry in pg_constraint catalog
    constraint_oid = CreateConstraintEntry(
        constr->conname,                    // Constraint name
        domainNamespace,                    // Namespace
        CONSTRAINT_NOTNULL,                 // Constraint type
        false,                              // Not deferrable
        false,                              // Not deferred
        !constr->skip_validation,           // Is validated
        InvalidOid,                         // No parent constraint
        InvalidOid,                         // Not a relation constraint
        NULL, 0, 0,                        // No key columns
        domainOid,                          // Domain constraint
        InvalidOid,                         // No associated index
        InvalidOid, NULL, NULL, NULL, NULL, // No foreign key info
        0, ' ', ' ', NULL, 0, ' ',          // No exclusion constraint info
        NULL,                               // No expression tree
        NULL,                               // No binary constraint
        true,                               // Is local
        0,                                  // No inheritance count
        false,                              // No inheritance restriction
        false);                             // Not internal

    if (constrAddr)
        ObjectAddressSet(*constrAddr, ConstraintRelationId, constraint_oid);
}
```