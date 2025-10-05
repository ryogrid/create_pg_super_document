# pg_relation_filepath

## Location
[src/backend/utils/adt/dbsize.c:948-1028](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/dbsize.c#L948-L1028)

## Overview
This PostgreSQL SQL function returns the filesystem pathname (relative to $PGDATA) of a relation given its OID, constructing the complete file path including tablespace and database directories.

## Definition
```c
Datum pg_relation_filepath(PG_FUNCTION_ARGS)
```

## Detailed Description
The `pg_relation_filepath` function provides the complete filesystem path for a relation's main data file. Similar to `pg_relation_filenode`, it works from the pg_class catalog for efficiency and handles various edge cases gracefully. The function constructs a complete RelFileLocator and determines the appropriate backend process number to build the full path.

Key operations:
1. **Catalog Lookup**: Retrieves relation information from pg_class
2. **Storage Check**: Verifies the relation has physical storage
3. **Location Construction**: Builds RelFileLocator with tablespace, database, and filenode
4. **Backend Determination**: Identifies owning backend for temporary relations
5. **Path Generation**: Uses relpathbackend to construct the full filesystem path

The function handles different relation persistence types (permanent, unlogged, temporary) and correctly resolves tablespace and database locations.

## Parameters / Member Variables
- Function accepts one argument via `PG_GETARG_OID(0)`: The OID of the relation whose filepath is requested

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID: Extracts OID argument from function call
  - [SearchSysCache1](../S/SearchSysCache1.md): Searches system cache for tuple by single key
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md): Converts OID to Datum
  - HeapTupleIsValid: Checks if heap tuple is valid
  - PG_RETURN_NULL: Returns NULL from function
  - GETSTRUCT: Extracts struct from heap tuple
  - RELKIND_HAS_STORAGE: Macro to check if relation kind has physical storage
  - [RelationMapOidToFilenumber](../R/RelationMapOidToFilenumber.md): Maps OID to filenode for mapped relations
  - RelFileNumberIsValid: Checks if file number is valid
  - [isTempOrTempToastNamespace](../i/isTempOrTempToastNamespace.md): Checks if namespace is for temporary relations
  - [ProcNumberForTempRelations](../P/ProcNumberForTempRelations.md): Gets process number for temp relations
  - [GetTempNamespaceProcNumber](../G/GetTempNamespaceProcNumber.md): Gets process number from namespace
  - [ReleaseSysCache](../R/ReleaseSysCache.md): Releases system cache tuple
  - [relpathbackend](../r/relpathbackend.md): Constructs relation file path
  - [cstring_to_text](../c/cstring_to_text.md): Converts C string to PostgreSQL text
  - PG_RETURN_TEXT_P: Returns text result from function
- Called from (representative examples):
  - No direct references found (likely called via SQL)

## Notes and Other Information
This function is designed to be called from SQL as `pg_relation_filepath(oid)`. It's commonly used for database administration, backup operations, and system analysis where the actual filesystem paths of relations need to be known. The function correctly handles different tablespace configurations, temporary relations, and mapped system catalogs. It returns the path relative to PGDATA, making it portable across different PostgreSQL installations. The function gracefully returns NULL for relations without storage or that cannot be found.

## Simplified Source

```c
Datum pg_relation_filepath(PG_FUNCTION_ARGS) {
    Oid relid = PG_GETARG_OID(0);
    HeapTuple tuple;
    Form_pg_class relform;
    RelFileLocator rlocator;
    ProcNumber backend;
    char *path;

    // Look up relation in pg_class catalog
    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tuple)) {
        PG_RETURN_NULL();
    }

    relform = (Form_pg_class) GETSTRUCT(tuple);

    // Check if relation has physical storage
    if (RELKIND_HAS_STORAGE(relform->relkind)) {
        // Set up RelFileLocator - tablespace, database, filenode
        if (relform->reltablespace) {
            rlocator.spcOid = relform->reltablespace;
        } else {
            rlocator.spcOid = MyDatabaseTableSpace;
        }

        if (rlocator.spcOid == GLOBALTABLESPACE_OID) {
            rlocator.dbOid = InvalidOid;
        } else {
            rlocator.dbOid = MyDatabaseId;
        }

        if (relform->relfilenode) {
            rlocator.relNumber = relform->relfilenode;
        } else {
            // Consult relation mapper for system catalogs
            rlocator.relNumber = RelationMapOidToFilenumber(relid, relform->relisshared);
        }
    } else {
        // No storage - return NULL
        rlocator.relNumber = InvalidRelFileNumber;
    }

    if (!RelFileNumberIsValid(rlocator.relNumber)) {
        ReleaseSysCache(tuple);
        PG_RETURN_NULL();
    }

    // Determine backend process for temporary relations
    switch (relform->relpersistence) {
        case RELPERSISTENCE_UNLOGGED:
        case RELPERSISTENCE_PERMANENT:
            backend = INVALID_PROC_NUMBER;
            break;
        case RELPERSISTENCE_TEMP:
            if (isTempOrTempToastNamespace(relform->relnamespace)) {
                backend = ProcNumberForTempRelations();
            } else {
                backend = GetTempNamespaceProcNumber(relform->relnamespace);
            }
            break;
        default:
            elog(ERROR, "invalid relpersistence: %c", relform->relpersistence);
            backend = INVALID_PROC_NUMBER;
            break;
    }

    ReleaseSysCache(tuple);

    // Construct the complete file path
    path = relpathbackend(rlocator, backend, MAIN_FORKNUM);

    PG_RETURN_TEXT_P(cstring_to_text(path));
}
```