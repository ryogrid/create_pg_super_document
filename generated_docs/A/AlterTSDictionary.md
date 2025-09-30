# AlterTSDictionary

## Location
[src/backend/commands/tsearchcmds.c:489-599](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tsearchcmds.c#L489-L599)

## Overview
Handles ALTER TEXT SEARCH DICTIONARY commands by modifying dictionary options while preserving the dictionary template.

## Definition
```c
ObjectAddress AlterTSDictionary(AlterTSDictionaryStmt *stmt)
```

## Detailed Description
AlterTSDictionary processes ALTER TEXT SEARCH DICTIONARY SQL commands by updating the dictionary's initialization options. It retrieves the existing options from the system catalog, modifies them according to the statement, validates the new options against the dictionary template, and updates the catalog. The function maintains dependency relationships and ensures proper ownership checking.

## Parameters / Member Variables
- `stmt`: AlterTSDictionaryStmt containing the dictionary name and option changes

## Dependencies
- Functions called/Symbols referenced:
  - get_ts_dict_oid (resolves dictionary name to OID)
  - object_ownercheck (verifies ownership permission)
  - SysCacheGetAttr (retrieves existing options)
  - deserialize_deflist (converts stored options to list)
  - verify_dictoptions (validates options against template)
  - serialize_deflist (converts options list for storage)
  - heap_modify_tuple (creates updated tuple)
  - CatalogTupleUpdate (updates system catalog)
  - InvokeObjectPostAlterHook (triggers post-alter hooks)
- Called from (representative examples):
  - ProcessUtilitySlow (SQL command processing)

## Notes and Other Information
- Only allows modification of dictionary options, not the template
- Requires ownership of the dictionary being altered
- Uses RowExclusiveLock on pg_ts_dict relation
- No dependency updates needed since only options are modified
- Part of PostgreSQL's text search infrastructure
- Validates all options against the dictionary template before applying changes

## Simplified Source

```c
ObjectAddress AlterTSDictionary(AlterTSDictionaryStmt *stmt) {
    HeapTuple tup, newtup;
    Relation rel;
    Oid dictId;
    List *dictoptions;
    Datum opt;
    bool isnull;
    ObjectAddress address;

    // Find the dictionary
    dictId = get_ts_dict_oid(stmt->dictname, false);
    rel = table_open(TSDictionaryRelationId, RowExclusiveLock);
    tup = SearchSysCache1(TSDICTOID, ObjectIdGetDatum(dictId));

    if (!HeapTupleIsValid(tup)) {
        elog(ERROR, "cache lookup failed for text search dictionary %u", dictId);
    }

    // Check ownership permission
    if (!object_ownercheck(TSDictionaryRelationId, dictId, GetUserId())) {
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TSDICTIONARY,
                       NameListToString(stmt->dictname));
    }

    // Get existing dictionary options
    opt = SysCacheGetAttr(TSDICTOID, tup, Anum_pg_ts_dict_dictinitoption, &isnull);
    if (isnull) {
        dictoptions = NIL;
    } else {
        dictoptions = deserialize_deflist(opt);
    }

    // Modify options list based on statement
    foreach(pl, stmt->options) {
        DefElem *defel = (DefElem *) lfirst(pl);

        // Remove any existing option with same name
        foreach(cell, dictoptions) {
            DefElem *oldel = (DefElem *) lfirst(cell);
            if (strcmp(oldel->defname, defel->defname) == 0) {
                dictoptions = foreach_delete_current(dictoptions, cell);
            }
        }

        // Add new value if provided
        if (defel->arg) {
            dictoptions = lappend(dictoptions, defel);
        }
    }

    // Validate new options against dictionary template
    verify_dictoptions(((Form_pg_ts_dict) GETSTRUCT(tup))->dicttemplate, dictoptions);

    // Update the catalog tuple
    memset(repl_val, 0, sizeof(repl_val));
    memset(repl_null, false, sizeof(repl_null));
    memset(repl_repl, false, sizeof(repl_repl));

    if (dictoptions) {
        repl_val[Anum_pg_ts_dict_dictinitoption - 1] =
            PointerGetDatum(serialize_deflist(dictoptions));
    } else {
        repl_null[Anum_pg_ts_dict_dictinitoption - 1] = true;
    }
    repl_repl[Anum_pg_ts_dict_dictinitoption - 1] = true;

    newtup = heap_modify_tuple(tup, RelationGetDescr(rel), repl_val, repl_null, repl_repl);
    CatalogTupleUpdate(rel, &newtup->t_self, newtup);

    // Trigger post-alter hooks
    InvokeObjectPostAlterHook(TSDictionaryRelationId, dictId, 0);
    ObjectAddressSet(address, TSDictionaryRelationId, dictId);

    // Cleanup
    heap_freetuple(newtup);
    ReleaseSysCache(tup);
    table_close(rel, RowExclusiveLock);

    return address;
}
```