# AlterDomainAddConstraint

## Location
[src/backend/commands/typecmds.c:2897-3036](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/typecmds.c#L2897-L3036)

## Overview
Implements the ALTER DOMAIN ADD CONSTRAINT statement, adding CHECK or NOT NULL constraints to domain types with proper validation and constraint enforcement.

## Definition

```c
ObjectAddress
AlterDomainAddConstraint(List *names, Node *newConstraint,
						 ObjectAddress *constrAddr)
```
## Detailed Description
This function adds constraints to domain types, supporting CHECK and NOT NULL constraint types while explicitly rejecting unsupported constraint types like UNIQUE, PRIMARY KEY, FOREIGN KEY, and EXCLUSION. For CHECK constraints, it processes the constraint expression, adds an entry to pg_constraint, and optionally validates existing data. For NOT NULL constraints, it sets the typnotnull flag and validates existing data unless validation is skipped. The function ensures proper cache invalidation for constraint changes that don't modify the pg_type row directly.

## Parameters / Member Variables
- : List of qualified names identifying the domain to modify
- : Node representing the constraint to add (must be a Constraint node)
- : Output parameter receiving the ObjectAddress of the created constraint

## Dependencies
- Functions called/Symbols referenced:
  - [makeTypeNameFromNameList](../m/makeTypeNameFromNameList.md)
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

## Simplified Source

```c
ObjectAddress
AlterDomainAddConstraint(List *names, Node *newConstraint, ObjectAddress *constrAddr)
{
    TypeName *typename;
    Oid domainoid;
    Relation typrel;
    HeapTuple tup;
    Form_pg_type typTup;
    Constraint *constr;
    char *ccbin;
    ObjectAddress address = InvalidObjectAddress;

    // Convert name list to typename and get domain OID
    typename = makeTypeNameFromNameList(names);
    domainoid = typenameTypeId(NULL, typename);

    // Open type catalog and get domain tuple
    typrel = table_open(TypeRelationId, RowExclusiveLock);
    tup = SearchSysCacheCopy1(TYPEOID, ObjectIdGetDatum(domainoid));
    if (!HeapTupleIsValid(tup))
        elog(ERROR, "cache lookup failed for type %u", domainoid);

    typTup = (Form_pg_type) GETSTRUCT(tup);

    // Check domain ownership permissions
    checkDomainOwner(tup);

    // Validate constraint node type
    if (!IsA(newConstraint, Constraint))
        elog(ERROR, "unrecognized node type: %d", (int) nodeTag(newConstraint));

    constr = (Constraint *) newConstraint;

    // Validate constraint type - only CHECK and NOT NULL are supported
    switch (constr->contype) {
        case CONSTR_CHECK:
        case CONSTR_NOTNULL:
            break; // Supported types
        case CONSTR_UNIQUE:
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("unique constraints not possible for domains")));
        case CONSTR_PRIMARY:
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("primary key constraints not possible for domains")));
        case CONSTR_EXCLUSION:
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("exclusion constraints not possible for domains")));
        case CONSTR_FOREIGN:
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("foreign key constraints not possible for domains")));
        default:
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("constraint type not supported for domains")));
    }

    if (constr->contype == CONSTR_CHECK) {
        // Add CHECK constraint to domain
        ccbin = domainAddCheckConstraint(domainoid, typTup->typnamespace,
                                       typTup->typbasetype, typTup->typtypmod,
                                       constr, NameStr(typTup->typname), constrAddr);

        // Validate existing data unless skip_validation is set
        if (!constr->skip_validation)
            validateDomainCheckConstraint(domainoid, ccbin);

        // Invalidate cache since pg_type row doesn't change
        CacheInvalidateHeapTuple(typrel, tup, NULL);

    } else if (constr->contype == CONSTR_NOTNULL) {
        // Check if domain is already NOT NULL
        if (typTup->typnotnull) {
            table_close(typrel, RowExclusiveLock);
            return address;
        }

        // Add NOT NULL constraint
        domainAddNotNullConstraint(domainoid, typTup->typnamespace,
                                 typTup->typbasetype, typTup->typtypmod,
                                 constr, NameStr(typTup->typname), constrAddr);

        // Validate existing data unless skip_validation is set
        if (!constr->skip_validation)
            validateDomainNotNullConstraint(domainoid);

        // Update typnotnull flag in pg_type
        typTup->typnotnull = true;
        CatalogTupleUpdate(typrel, &tup->t_self, tup);
    }

    ObjectAddressSet(address, TypeRelationId, domainoid);

    // Clean up
    table_close(typrel, RowExclusiveLock);

    return address;
}
```