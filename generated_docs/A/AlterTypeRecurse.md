# AlterTypeRecurse

## Location
[src/backend/commands/typecmds.c:4563-4707](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/typecmds.c#L4563-L4707)

## Overview
A recursive function that applies type property changes to a base type and automatically propagates appropriate changes to its array type and all domains built on top of it.

## Definition

```c
static void
AlterTypeRecurse(Oid typeOid, bool isImplicitArray,
				 HeapTuple tup, Relation catalog,
				 AlterTypeRecurseParams *atparams)
```
## Detailed Description
AlterTypeRecurse performs the actual catalog updates for type property modifications and ensures consistency across related types through recursive propagation. It updates the pg_type tuple for the specified type, regenerates type dependencies, and then recursively processes the associated array type (for typmod functions only) and all domains that use this type as their base. The function implements PostgreSQL's type inheritance model where domains inherit most properties from their base types, while arrays inherit only typmod-related functions.

## Parameters / Member Variables
- `typeOid`: OID of the type being modified
- `isImplicitArray`: Boolean flag indicating if this is an internal call for processing an array type
- `tup`: HeapTuple containing the current pg_type row for the type
- `catalog`: Open relation handle for the pg_type catalog
- `*atparams`: AlterTypeRecurseParams structure containing all the property changes to apply
## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md)
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [GenerateTypeDependencies](../G/GenerateTypeDependencies.md)
  - InvokeObjectPostAlterHook
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [systable_endscan](../s/systable_endscan.md)
  - [AlterTypeRecurse](AlterTypeRecurse.md) (recursive call)
- Called from (representative examples):
  - [AlterType](AlterType.md)
  - [AlterTypeRecurse](AlterTypeRecurse.md) (recursive call)

## Notes and Other Information
- Includes stack depth checking to prevent overflow during deep recursion
- Updates only the pg_type attributes that are flagged for change in atparams
- Rebuilds type dependencies after catalog updates to maintain referential integrity
- Arrays inherit only typmodin and typmodout functions from their base type
- Domains inherit storage, send, and analyze functions but not receive, typmod, or subscript functions
- Uses a system catalog scan to find all domains with the current type as their base type
- Handles race conditions gracefully - concurrent domain creation might be missed but can be fixed by re-running the ALTER TYPE command
- Automatically filters the inheritance for domains by clearing flags for non-inherited properties

## Simplified Source

```c
static void AlterTypeRecurse(Oid typeOid, bool isImplicitArray, HeapTuple tup,
                            Relation catalog, AlterTypeRecurseParams *atparams) {
    check_stack_depth();

    // Update current type's pg_type tuple with new properties
    Datum values[Natts_pg_type];
    bool nulls[Natts_pg_type];
    bool replaces[Natts_pg_type];

    memset(values, 0, sizeof(values));
    memset(nulls, 0, sizeof(nulls));
    memset(replaces, 0, sizeof(replaces));

    // Set values for each property being updated
    if (atparams->updateStorage) {
        replaces[Anum_pg_type_typstorage - 1] = true;
        values[Anum_pg_type_typstorage - 1] = CharGetDatum(atparams->storage);
    }
    // ... similar blocks for other properties (receive, send, typmodin, etc.)

    // Apply changes to catalog
    HeapTuple newtup = heap_modify_tuple(tup, RelationGetDescr(catalog),
                                        values, nulls, replaces);
    CatalogTupleUpdate(catalog, &newtup->t_self, newtup);

    // Rebuild type dependencies
    GenerateTypeDependencies(newtup, catalog, NULL, NULL, 0,
                           isImplicitArray, isImplicitArray, false, true);

    InvokeObjectPostAlterHook(TypeRelationId, typeOid, 0);

    // Recursively update array type (typmod functions only)
    if (!isImplicitArray && (atparams->updateTypmodin || atparams->updateTypmodout)) {
        Oid arrtypoid = ((Form_pg_type) GETSTRUCT(newtup))->typarray;
        if (OidIsValid(arrtypoid)) {
            HeapTuple arrtup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(arrtypoid));
            AlterTypeRecurseParams arrparams = {0};
            arrparams.updateTypmodin = atparams->updateTypmodin;
            arrparams.updateTypmodout = atparams->updateTypmodout;
            arrparams.typmodinOid = atparams->typmodinOid;
            arrparams.typmodoutOid = atparams->typmodoutOid;

            AlterTypeRecurse(arrtypoid, true, arrtup, catalog, &arrparams);
            ReleaseSysCache(arrtup);
        }
    }

    // Filter properties not inherited by domains
    atparams->updateReceive = false;
    atparams->updateTypmodin = false;
    atparams->updateTypmodout = false;
    atparams->updateSubscript = false;

    // Recursively update all domains using this type as base
    if (atparams->updateStorage || atparams->updateSend || atparams->updateAnalyze) {
        ScanKeyData key[1];
        ScanKeyInit(&key[0], Anum_pg_type_typbasetype, BTEqualStrategyNumber,
                   F_OIDEQ, ObjectIdGetDatum(typeOid));

        SysScanDesc scan = systable_beginscan(catalog, InvalidOid, false, NULL, 1, key);
        HeapTuple domainTup;

        while ((domainTup = systable_getnext(scan)) != NULL) {
            Form_pg_type domainForm = (Form_pg_type) GETSTRUCT(domainTup);
            if (domainForm->typtype == TYPTYPE_DOMAIN) {
                AlterTypeRecurse(domainForm->oid, false, domainTup, catalog, atparams);
            }
        }
        systable_endscan(scan);
    }
}
```