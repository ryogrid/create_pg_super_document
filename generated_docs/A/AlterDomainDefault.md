# AlterDomainDefault

## Location
[src/backend/commands/typecmds.c:2576-2704](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/typecmds.c#L2576-L2704)

## Overview
AlterDomainDefault implements the ALTER DOMAIN SET/DROP DEFAULT statements, allowing users to modify or remove the default value for an existing domain type in PostgreSQL.

## Definition
```c
ObjectAddress AlterDomainDefault(List *names, Node *defaultRaw)
```

## Detailed Description
This function handles the modification of default values for domain types in PostgreSQL. Domains are user-defined types based on existing types with optional constraints and default values. The function supports both setting a new default value and dropping an existing default.

The function performs several key operations:
1. **Type Resolution**: Converts the name list to a typename and resolves the domain OID
2. **Permission Checking**: Verifies the user has ownership rights to alter the domain
3. **Expression Processing**: If setting a default, parses and validates the default expression
4. **Catalog Update**: Updates the pg_type system catalog with the new default information
5. **Dependency Management**: Rebuilds dependencies to maintain referential integrity

The function handles special cases like NULL defaults (treated as DROP DEFAULT) and ensures that both binary (typdefaultbin) and textual (typdefault) representations of defaults are properly maintained.

## Parameters / Member Variables
- `names`: `List *` - Qualified name list identifying the domain to alter
- `defaultRaw`: `Node *` - The new default expression (NULL for DROP DEFAULT)
- **Return value**: `ObjectAddress` - Address of the modified domain object

## Dependencies
- Functions called/Symbols referenced:
  - `[makeTypeNameFromNameList](../m/makeTypeNameFromNameList.md)` (name resolution)
  - [typenameTypeId](../t/typenameTypeId.md) (type OID lookup)
  - `[table_open](../t/table_open.md)` (catalog access)
  - `SearchSysCacheCopy1` (domain tuple lookup)
  - [checkDomainOwner](../c/checkDomainOwner.md) (permission validation)
  - [make_parsestate](../m/make_parsestate.md) (expression parsing setup)
  - [cookDefault](../c/cookDefault.md) (default expression processing)
  - [deparse_expression](../d/deparse_expression.md) (expression to text conversion)
  - [nodeToString](../n/nodeToString.md) (AST serialization)
  - [heap_modify_tuple](../h/heap_modify_tuple.md) (tuple modification)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md) (catalog update)
  - [GenerateTypeDependencies](../G/GenerateTypeDependencies.md) (dependency rebuilding)
  - `InvokeObjectPostAlterHook` (post-alter hooks)
  - `ObjectAddressSet` (result address setup)
  - [heap_freetuple](../h/heap_freetuple.md) (memory cleanup)
  
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (src/backend/tcop/utility.c:1350)

## Notes and Other Information
- The function maintains both binary (typdefaultbin) and textual (typdefault) representations of the default value
- NULL constants in default expressions are treated as requests to drop the default
- Uses RowExclusiveLock on the pg_type catalog to prevent concurrent modifications
- Rebuilds type dependencies to ensure referential integrity after the change
- The function validates the default expression against the domain's base type and type modifier
- Supports the full expression syntax for defaults, not just simple constants
- Properly handles both SET DEFAULT and DROP DEFAULT variants of the ALTER DOMAIN command
- The textual representation is maintained primarily for pg_dump compatibility
- Post-alter hooks are invoked to allow extensions to respond to the domain modification

## Simplified Source

```c
ObjectAddress
AlterDomainDefault(List *names, Node *defaultRaw)
{
    TypeName *typename;
    Oid domainoid;
    HeapTuple tup;
    ParseState *pstate;
    Relation rel;
    char *defaultValue;
    Node *defaultExpr = NULL;
    Datum new_record[Natts_pg_type] = {0};
    bool new_record_nulls[Natts_pg_type] = {0};
    bool new_record_repl[Natts_pg_type] = {0};
    HeapTuple newtuple;
    Form_pg_type typTup;
    ObjectAddress address;

    // Convert name list to typename and resolve domain OID
    typename = makeTypeNameFromNameList(names);
    domainoid = typenameTypeId(NULL, typename);

    // Open type catalog and get domain tuple
    rel = table_open(TypeRelationId, RowExclusiveLock);
    tup = SearchSysCacheCopy1(TYPEOID, ObjectIdGetDatum(domainoid));
    if (!HeapTupleIsValid(tup))
        elog(ERROR, "cache lookup failed for type %u", domainoid);

    typTup = (Form_pg_type) GETSTRUCT(tup);

    // Check domain ownership permissions
    checkDomainOwner(tup);

    if (defaultRaw) {
        // SET DEFAULT case: process the new default expression
        pstate = make_parsestate(NULL);

        // Cook the raw expression into a validated default expression
        defaultExpr = cookDefault(pstate, defaultRaw, typTup->typbasetype,
                                typTup->typtypmod, NameStr(typTup->typname), 0);

        // Handle NULL defaults as DROP DEFAULT
        if (defaultExpr == NULL ||
            (IsA(defaultExpr, Const) && ((Const *) defaultExpr)->constisnull)) {
            // Drop the default
            new_record_nulls[Anum_pg_type_typdefaultbin - 1] = true;
            new_record_repl[Anum_pg_type_typdefaultbin - 1] = true;
            new_record_nulls[Anum_pg_type_typdefault - 1] = true;
            new_record_repl[Anum_pg_type_typdefault - 1] = true;
        } else {
            // Set the new default - store both binary and textual forms
            defaultValue = deparse_expression(defaultExpr, NIL, false, false);

            new_record[Anum_pg_type_typdefaultbin - 1] = CStringGetTextDatum(nodeToString(defaultExpr));
            new_record_repl[Anum_pg_type_typdefaultbin - 1] = true;
            new_record[Anum_pg_type_typdefault - 1] = CStringGetTextDatum(defaultValue);
            new_record_repl[Anum_pg_type_typdefault - 1] = true;
        }
    } else {
        // DROP DEFAULT case: clear both default fields
        new_record_nulls[Anum_pg_type_typdefaultbin - 1] = true;
        new_record_repl[Anum_pg_type_typdefaultbin - 1] = true;
        new_record_nulls[Anum_pg_type_typdefault - 1] = true;
        new_record_repl[Anum_pg_type_typdefault - 1] = true;
    }

    // Update the catalog tuple
    newtuple = heap_modify_tuple(tup, RelationGetDescr(rel),
                               new_record, new_record_nulls, new_record_repl);
    CatalogTupleUpdate(rel, &tup->t_self, newtuple);

    // Rebuild dependencies for the modified domain
    GenerateTypeDependencies(newtuple, rel, defaultExpr, NULL, 0, false, false, false, true);

    // Invoke post-alter hooks
    InvokeObjectPostAlterHook(TypeRelationId, domainoid, 0);

    ObjectAddressSet(address, TypeRelationId, domainoid);

    // Clean up
    table_close(rel, RowExclusiveLock);
    heap_freetuple(newtuple);

    return address;
}
```