# AlterType

## Location
[src/backend/commands/typecmds.c:4312-4562](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/typecmds.c#L4312-L4562)

## Overview
The main entry point for executing ALTER TYPE SET commands that modify various properties of PostgreSQL base types, with strict validation and security controls.

## Definition

```c
ObjectAddress
AlterType(AlterTypeStmt *stmt)
```
## Detailed Description
AlterType processes ALTER TYPE SET commands that can modify specific properties of base types including storage strategy, I/O functions (receive, send, typmod_in, typmod_out), analysis function, and subscript function. The function enforces strict limitations, allowing changes only to base types (not composite types, domains, or arrays) and requiring superuser privileges for I/O function modifications. It validates all requested changes, builds a parameters structure, and delegates the actual recursive modification to AlterTypeRecurse.

## Parameters / Member Variables
- `*stmt`: AlterTypeStmt structure containing the type name and list of property modifications to apply
## Dependencies
- Functions called/Symbols referenced:
  - [makeTypeNameFromNameList](../m/makeTypeNameFromNameList.md)
  - [typenameType](../t/typenameType.md)
  - [typeTypeId](../t/typeTypeId.md)
  - [defGetString](../d/defGetString.md)
  - [defGetQualifiedName](../d/defGetQualifiedName.md)
  - [findTypeReceiveFunction](../f/findTypeReceiveFunction.md)
  - [findTypeSendFunction](../f/findTypeSendFunction.md)
  - [findTypeTypmodinFunction](../f/findTypeTypmodinFunction.md)
  - [findTypeTypmodoutFunction](../f/findTypeTypmodoutFunction.md)
  - [findTypeAnalyzeFunction](../f/findTypeAnalyzeFunction.md)
  - [findTypeSubscriptingFunction](../f/findTypeSubscriptingFunction.md)
  - [superuser](../s/superuser.md)
  - [object_ownercheck](../o/object_ownercheck.md)
  - [aclcheck_error_type](../a/aclcheck_error_type.md)
  - IsTrueArrayType
  - [AlterTypeRecurse](AlterTypeRecurse.md)
  - ObjectAddressSet
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
- Only allows modification of base types, rejecting composite types, domains, and array types
- Requires superuser privileges for changing I/O functions due to security implications
- Validates storage changes, preventing transitions from non-PLAIN to PLAIN storage
- Explicitly rejects modification of immutable type properties like input/output functions, internal length, and alignment
- Uses AlterTypeRecurseParams structure to pass modification parameters to the recursive function
- Returns ObjectAddress of the modified type for dependency tracking and further processing
- Enforces that fixed-size types (typlen != -1) can only use PLAIN storage

## Simplified Source

```c
ObjectAddress AlterType(AlterTypeStmt *stmt)
{
    ObjectAddress address;
    Relation    catalog;
    TypeName   *typename;
    HeapTuple   tup;
    Oid         typeOid;
    Form_pg_type typForm;
    bool        requireSuper = false;
    AlterTypeRecurseParams atparams;
    ListCell   *pl;

    // Open the type catalog and look up the type
    catalog = table_open(TypeRelationId, RowExclusiveLock);
    typename = makeTypeNameFromNameList(stmt->typeName);
    tup = typenameType(NULL, typename, NULL);

    typeOid = typeTypeId(tup);
    typForm = (Form_pg_type) GETSTRUCT(tup);

    // Process each option in the ALTER TYPE SET command
    memset(&atparams, 0, sizeof(atparams));
    foreach(pl, stmt->options)
    {
        DefElem *defel = (DefElem *) lfirst(pl);

        if (strcmp(defel->defname, "storage") == 0)
        {
            // Parse storage type (plain, external, extended, main)
            char *a = defGetString(defel);

            if (pg_strcasecmp(a, "plain") == 0)
                atparams.storage = TYPSTORAGE_PLAIN;
            else if (pg_strcasecmp(a, "external") == 0)
                atparams.storage = TYPSTORAGE_EXTERNAL;
            else if (pg_strcasecmp(a, "extended") == 0)
                atparams.storage = TYPSTORAGE_EXTENDED;
            else if (pg_strcasecmp(a, "main") == 0)
                atparams.storage = TYPSTORAGE_MAIN;
            else
                ereport(ERROR, "storage type not recognized");

            // Validate storage change rules
            if (atparams.storage != TYPSTORAGE_PLAIN && typForm->typlen != -1)
                ereport(ERROR, "fixed-size types must have storage PLAIN");

            if (atparams.storage != TYPSTORAGE_PLAIN && typForm->typstorage == TYPSTORAGE_PLAIN)
                requireSuper = true;  // Need superuser to change from PLAIN
            else if (atparams.storage == TYPSTORAGE_PLAIN && typForm->typstorage != TYPSTORAGE_PLAIN)
                ereport(ERROR, "cannot change type's storage to PLAIN");

            atparams.updateStorage = true;
        }
        else if (strcmp(defel->defname, "receive") == 0)
        {
            // Set receive function
            if (defel->arg != NULL)
                atparams.receiveOid = findTypeReceiveFunction(defGetQualifiedName(defel), typeOid);
            else
                atparams.receiveOid = InvalidOid;  // Remove function
            atparams.updateReceive = true;
            requireSuper = true;  // I/O functions need superuser
        }
        else if (strcmp(defel->defname, "send") == 0)
        {
            // Set send function
            if (defel->arg != NULL)
                atparams.sendOid = findTypeSendFunction(defGetQualifiedName(defel), typeOid);
            else
                atparams.sendOid = InvalidOid;  // Remove function
            atparams.updateSend = true;
            requireSuper = true;
        }
        // ... similar handling for typmod_in, typmod_out, analyze, subscript functions
        else if (/* immutable attributes like input, output, internallength, etc. */)
        {
            ereport(ERROR, "type attribute cannot be changed");
        }
        else
        {
            ereport(ERROR, "type attribute not recognized");
        }
    }

    // Check permissions - superuser for dangerous operations, owner otherwise
    if (requireSuper)
    {
        if (!superuser())
            ereport(ERROR, "must be superuser to alter a type");
    }
    else
    {
        if (!object_ownercheck(TypeRelationId, typeOid, GetUserId()))
            aclcheck_error_type(ACLCHECK_NOT_OWNER, typeOid);
    }

    // Only allow changes to base types, not composite/domain/array types
    if (typForm->typtype != TYPTYPE_BASE)
        ereport(ERROR, "not a base type");

    if (IsTrueArrayType(typForm))
        ereport(ERROR, "not a base type");

    // Apply the changes recursively to this type and any dependent types
    AlterTypeRecurse(typeOid, false, tup, catalog, &atparams);

    // Clean up and return
    ReleaseSysCache(tup);
    table_close(catalog, RowExclusiveLock);

    ObjectAddressSet(address, TypeRelationId, typeOid);
    return address;
}
```