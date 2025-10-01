# ScanSourceDatabasePgClassTuple

## Location
[src/backend/commands/dbcommands.c:391-455](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/dbcommands.c#L391-L455)

## Overview
ScanSourceDatabasePgClassTuple examines a pg_class tuple to determine if it represents a relation that needs to be copied during database creation and constructs a CreateDBRelInfo structure if copying is required.

## Definition

```c
enumber = InvalidRelFileNumber;
```
## Detailed Description
This function analyzes the contents of a pg_class tuple to make decisions about whether the corresponding relation needs to be copied to the destination database. The function performs several filtering checks:

1. **Shared objects**: Relations in GLOBALTABLESPACE_OID are shared across all databases and don't need copying
2. **Storage requirements**: Relations without storage (views, etc.) using RELKIND_HAS_STORAGE check
3. **Temporary relations**: Relations with RELPERSISTENCE_TEMP are session-specific and inaccessible

For relations that pass these filters, the function:
- Determines the relfilenumber either directly from classForm->relfilenode or via RelationMapOidToFilenumberForDatabase for mapped relations
- Validates that a valid relfilenumber exists
- Creates and populates a CreateDBRelInfo structure with:
  - Tablespace OID (using relation's tablespace or default tbid)
  - Database OID 
  - Relation file number
  - Relation OID
  - Permanence flag based on persistence type

## Parameters / Member Variables
- : HeapTupleData pointer containing the pg_class tuple to analyze
- : Tablespace ID of the source database's default tablespace  
- : Database ID of the source database
- : Filesystem path to the source database directory

## Dependencies
- Functions called/Symbols referenced:
  - GETSTRUCT: Macro to extract Form_pg_class from heap tuple
  - RELKIND_HAS_STORAGE: Macro to check if relation kind has storage
  - RelFileNumberIsValid: Validates relation file numbers
  - [RelationMapOidToFilenumberForDatabase](../R/RelationMapOidToFilenumberForDatabase.md): Maps relation OID to file number for mapped relations
  - OidIsValid: Validates OID values
  - [palloc](../p/palloc.md): Allocates memory for CreateDBRelInfo structure
- Called from (representative examples):
  - [ScanSourceDatabasePgClassPage](ScanSourceDatabasePgClassPage.md): Uses this to process individual pg_class tuples

## Notes and Other Information
- Returns NULL for relations that don't need copying (shared, no storage, temporary)
- Handles both regular relations (with valid relfilenode) and mapped relations (requiring relmap lookup)
- Properly maps tablespace OIDs, using the default tablespace ID when relation has no explicit tablespace
- Validates that all relations have valid file numbers, throwing ERROR if not found
- Sets permanence flag based on relpersistence field (permanent vs unlogged)
- Part of the database creation process that determines which relations to copy
- Located at src/backend/commands/dbcommands.c:391-455

## Simplified Source

```c
CreateDBRelInfo *ScanSourceDatabasePgClassTuple(HeapTupleData *tuple, Oid tbid, Oid dbid, char *srcpath)
{
    CreateDBRelInfo *relinfo;
    Form_pg_class classForm;
    RelFileNumber relfilenumber = InvalidRelFileNumber;

    classForm = (Form_pg_class) GETSTRUCT(tuple);

    // Skip objects that don't need copying
    if (classForm->reltablespace == GLOBALTABLESPACE_OID ||      // Shared objects
        !RELKIND_HAS_STORAGE(classForm->relkind) ||             // No storage
        classForm->relpersistence == RELPERSISTENCE_TEMP)       // Temporary
        return NULL;

    // Get relation file number
    if (RelFileNumberIsValid(classForm->relfilenode))
        relfilenumber = classForm->relfilenode;
    else
        relfilenumber = RelationMapOidToFilenumberForDatabase(srcpath, classForm->oid);

    if (!RelFileNumberIsValid(relfilenumber))
        elog(ERROR, "relation with OID %u does not have a valid relfilenumber", classForm->oid);

    // Create relation info structure
    relinfo = (CreateDBRelInfo *) palloc(sizeof(CreateDBRelInfo));

    // Set tablespace (use relation's or default)
    if (OidIsValid(classForm->reltablespace))
        relinfo->rlocator.spcOid = classForm->reltablespace;
    else
        relinfo->rlocator.spcOid = tbid;

    relinfo->rlocator.dbOid = dbid;
    relinfo->rlocator.relNumber = relfilenumber;
    relinfo->reloid = classForm->oid;
    relinfo->permanent = (classForm->relpersistence == RELPERSISTENCE_PERMANENT);

    return relinfo;
}
```