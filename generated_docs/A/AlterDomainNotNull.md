# AlterDomainNotNull

## Location
[src/backend/commands/typecmds.c:2705-2790](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/typecmds.c#L2705-L2790)

## Overview
Implements the ALTER DOMAIN SET/DROP NOT NULL statements, managing the NOT NULL constraint on domain types by adding or removing the constraint and updating the domain's metadata.

## Definition

```c
ObjectAddress
AlterDomainNotNull(List *names, bool notNull)
```
## Detailed Description
This function handles both setting and dropping NOT NULL constraints on domain types. When setting a NOT NULL constraint (notNull=true), it creates a new constraint node, adds it to the domain using domainAddNotNullConstraint, and validates existing data. When dropping the constraint (notNull=false), it finds the existing NOT NULL constraint and performs deletion. The function updates the pg_type catalog to reflect the new constraint state and returns the ObjectAddress of the modified domain.

## Parameters / Member Variables
- : List of qualified names identifying the domain to alter
- : Boolean flag indicating whether to set (true) or drop (false) the NOT NULL constraint

## Dependencies
- Functions called/Symbols referenced:
  - [makeTypeNameFromNameList](../m/makeTypeNameFromNameList.md)
  - [typenameTypeId](../t/typenameTypeId.md)
  - SearchSysCacheCopy1
  - [checkDomainOwner](../c/checkDomainOwner.md)
  - [domainAddNotNullConstraint](../d/domainAddNotNullConstraint.md)
  - [validateDomainNotNullConstraint](../v/validateDomainNotNullConstraint.md)
  - [findDomainNotNullConstraint](../f/findDomainNotNullConstraint.md)
  - [performDeletion](../p/performDeletion.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - InvokeObjectPostAlterHook
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
- Returns early if the domain already has the desired constraint state
- Uses RowExclusiveLock on the type relation to ensure consistency
- Validates existing table data when adding NOT NULL constraints
- Properly handles cleanup of catalog tuples and relation locks
- Triggers post-alter hooks for proper event notification

## Simplified Source

```c
ObjectAddress
AlterDomainNotNull(List *names, bool notNull)
{
    TypeName *typename;
    Oid domainoid;
    Relation typrel;
    HeapTuple tup;
    Form_pg_type typTup;
    ObjectAddress address = InvalidObjectAddress;

    // Convert name list to typename and resolve domain OID
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

    // Early return if domain already has desired constraint state
    if (typTup->typnotnull == notNull) {
        table_close(typrel, RowExclusiveLock);
        return address;
    }

    if (notNull) {
        // SET NOT NULL case: create and add constraint
        Constraint *constr;

        constr = makeNode(Constraint);
        constr->contype = CONSTR_NOTNULL;
        constr->initially_valid = true;
        constr->location = -1;

        // Add the NOT NULL constraint
        domainAddNotNullConstraint(domainoid, typTup->typnamespace,
                                 typTup->typbasetype, typTup->typtypmod,
                                 constr, NameStr(typTup->typname), NULL);

        // Validate existing data
        validateDomainNotNullConstraint(domainoid);

    } else {
        // DROP NOT NULL case: find and remove constraint
        HeapTuple conTup;
        ObjectAddress conobj;

        conTup = findDomainNotNullConstraint(domainoid);
        if (conTup == NULL)
            elog(ERROR, "could not find not-null constraint on domain \"%s\"",
                 NameStr(typTup->typname));

        ObjectAddressSet(conobj, ConstraintRelationId,
                        ((Form_pg_constraint) GETSTRUCT(conTup))->oid);
        performDeletion(&conobj, DROP_RESTRICT, 0);
    }

    // Update pg_type row with new constraint state
    typTup->typnotnull = notNull;
    CatalogTupleUpdate(typrel, &tup->t_self, tup);

    // Trigger post-alter hooks
    InvokeObjectPostAlterHook(TypeRelationId, domainoid, 0);

    ObjectAddressSet(address, TypeRelationId, domainoid);

    // Clean up
    heap_freetuple(tup);
    table_close(typrel, RowExclusiveLock);

    return address;
}
```