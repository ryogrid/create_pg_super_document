# CreateProceduralLanguage

## Location
[src/backend/commands/proclang.c:37-225](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/proclang.c#L37-L225)

## Overview
Creates a new procedural language or replaces an existing one in the PostgreSQL system, handling all aspects of language definition including handler functions, validation, and dependency management.

## Definition

```c
ObjectAddress
CreateProceduralLanguage(CreatePLangStmt *stmt)
```
## Detailed Description
This function implements the CREATE LANGUAGE SQL command functionality. It validates the language definition, creates or updates the pg_language catalog entry, and establishes proper dependencies. The function performs comprehensive validation of handler functions, manages ownership and permissions, and ensures proper catalog consistency.

Key operations include:
- Superuser privilege verification
- Handler function validation and type checking
- Optional inline and validator function validation
- Catalog entry creation or update with proper locking
- Dependency record management for proper cleanup
- Extension membership recording
- Post-creation hook invocation

The function supports both creating new languages and replacing existing ones when the REPLACE option is specified.

## Parameters / Member Variables
- `*stmt`: Pointer to CreatePLangStmt containing the parsed CREATE LANGUAGE statement with all language definition details including name, handler function, trust level, and optional inline/validator functions
## Dependencies
- Functions called/Symbols referenced:
  - [superuser](../s/superuser.md)
  - [LookupFuncName](../L/LookupFuncName.md)
  - [get_func_rettype](../g/get_func_rettype.md)
  - [NameListToString](../N/NameListToString.md)
  - [table_open](../t/table_open.md)
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](CatalogTupleUpdate.md)
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [CatalogTupleInsert](CatalogTupleInsert.md)
  - [deleteDependencyRecordsFor](../d/deleteDependencyRecordsFor.md)
  - [recordDependencyOnOwner](../r/recordDependencyOnOwner.md)
  - [recordDependencyOnCurrentExtension](../r/recordDependencyOnCurrentExtension.md)
  - [record_object_address_dependencies](../r/record_object_address_dependencies.md)
  - InvokeObjectPostCreateHook
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
- Requires superuser privileges to create custom procedural languages
- Handler function must return language_handler type
- Inline function (if specified) must accept internal type parameter
- Validator function (if specified) must accept oid type parameter
- When replacing existing language, preserves OID, ownership, and ACL permissions
- Creates dependencies on handler, inline, and validator functions to ensure proper cleanup
- Automatically records extension membership if created within an extension context
- Function is located in src/backend/commands/proclang.c:37-225

## Simplified Source

```c
ObjectAddress
CreateProceduralLanguage(CreatePLangStmt *stmt)
{
    const char *languageName = stmt->plname;
    Oid languageOwner = GetUserId();
    Oid handlerOid, inlineOid, valOid;
    Relation rel;
    HeapTuple oldtup, tup;
    Oid langoid;
    bool is_update;
    ObjectAddress myself;

    // Check superuser permission
    if (!superuser())
        ereport(ERROR, "must be superuser to create custom procedural language");

    // Validate handler function and check return type
    handlerOid = LookupFuncName(stmt->plhandler, 0, NULL, false);
    if (get_func_rettype(handlerOid) != LANGUAGE_HANDLEROID)
        ereport(ERROR, "function must return type language_handler");

    // Validate optional inline function
    if (stmt->plinline) {
        inlineOid = LookupFuncName(stmt->plinline, 1, funcargtypes, false);
    } else {
        inlineOid = InvalidOid;
    }

    // Validate optional validator function
    if (stmt->plvalidator) {
        valOid = LookupFuncName(stmt->plvalidator, 1, funcargtypes, false);
    } else {
        valOid = InvalidOid;
    }

    // Open language catalog
    rel = table_open(LanguageRelationId, RowExclusiveLock);

    // Prepare tuple data
    memset(values, 0, sizeof(values));
    memset(nulls, false, sizeof(nulls));
    memset(replaces, true, sizeof(replaces));

    // Set language attributes
    values[Anum_pg_language_lanname - 1] = NameGetDatum(&langname);
    values[Anum_pg_language_lanowner - 1] = ObjectIdGetDatum(languageOwner);
    values[Anum_pg_language_lanispl - 1] = BoolGetDatum(true);
    values[Anum_pg_language_lanpltrusted - 1] = BoolGetDatum(stmt->pltrusted);
    values[Anum_pg_language_lanplcallfoid - 1] = ObjectIdGetDatum(handlerOid);
    values[Anum_pg_language_laninline - 1] = ObjectIdGetDatum(inlineOid);
    values[Anum_pg_language_lanvalidator - 1] = ObjectIdGetDatum(valOid);

    // Check if language already exists
    oldtup = SearchSysCache1(LANGNAME, PointerGetDatum(languageName));

    if (HeapTupleIsValid(oldtup)) {
        // Update existing language if REPLACE specified
        if (!stmt->replace)
            ereport(ERROR, "language already exists");

        // Preserve ownership and permissions
        replaces[Anum_pg_language_oid - 1] = false;
        replaces[Anum_pg_language_lanowner - 1] = false;
        replaces[Anum_pg_language_lanacl - 1] = false;

        tup = heap_modify_tuple(oldtup, tupDesc, values, nulls, replaces);
        CatalogTupleUpdate(rel, &tup->t_self, tup);
        langoid = ((Form_pg_language) GETSTRUCT(oldtup))->oid;
        is_update = true;
    } else {
        // Create new language
        langoid = GetNewOidWithIndex(rel, LanguageOidIndexId, Anum_pg_language_oid);
        values[Anum_pg_language_oid - 1] = ObjectIdGetDatum(langoid);
        tup = heap_form_tuple(tupDesc, values, nulls);
        CatalogTupleInsert(rel, tup);
        is_update = false;
    }

    // Set up dependencies
    myself.classId = LanguageRelationId;
    myself.objectId = langoid;
    myself.objectSubId = 0;

    if (is_update)
        deleteDependencyRecordsFor(myself.classId, myself.objectId, true);

    if (!is_update)
        recordDependencyOnOwner(myself.classId, myself.objectId, languageOwner);

    recordDependencyOnCurrentExtension(&myself, is_update);

    // Create dependencies on handler and optional functions
    addrs = new_object_addresses();
    add_exact_object_address(&(ObjectAddress){ProcedureRelationId, handlerOid}, addrs);

    if (OidIsValid(inlineOid))
        add_exact_object_address(&(ObjectAddress){ProcedureRelationId, inlineOid}, addrs);

    if (OidIsValid(valOid))
        add_exact_object_address(&(ObjectAddress){ProcedureRelationId, valOid}, addrs);

    record_object_address_dependencies(&myself, addrs, DEPENDENCY_NORMAL);

    // Cleanup and finish
    InvokeObjectPostCreateHook(LanguageRelationId, myself.objectId, 0);
    table_close(rel, RowExclusiveLock);

    return myself;
}
```