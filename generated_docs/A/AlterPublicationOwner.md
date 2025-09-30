# AlterPublicationOwner

## Location
[src/backend/commands/publicationcmds.c:1946-1980](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/publicationcmds.c#L1946-L1980)

## Overview
Changes the ownership of a PostgreSQL publication identified by name, serving as the public interface for publication ownership changes.

## Definition
```c
ObjectAddress AlterPublicationOwner(const char *name, Oid newOwnerId)
```

## Detailed Description
This function serves as the public interface for changing publication ownership when the publication is identified by name. It handles the complete workflow of publication lookup, validation, and ownership transfer. The function opens the pg_publication system catalog, searches for the named publication, and delegates the actual ownership change logic to AlterPublicationOwner_internal. It properly manages catalog access with appropriate locking and ensures memory cleanup.

The function returns an ObjectAddress representing the modified publication, which can be used for further operations or event tracking. It includes comprehensive error handling for non-existent publications.

## Parameters / Member Variables
- `name`: Name of the publication whose ownership will be changed
- `newOwnerId`: OID of the user who will become the new owner of the publication

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - SearchSysCacheCopy1
  - [CStringGetDatum](../C/CStringGetDatum.md)
  - [AlterPublicationOwner_internal](AlterPublicationOwner_internal.md)
  - ObjectAddressSet
  - [heap_freetuple](../h/heap_freetuple.md)
  - [table_close](../t/table_close.md)
  - Form_pg_publication (struct type)
- Called from (representative examples):
  - [ExecAlterOwnerStmt](../E/ExecAlterOwnerStmt.md)

## Notes and Other Information
- This is a public function, accessible throughout the PostgreSQL codebase and declared in publicationcmds.h
- Uses RowExclusiveLock on the pg_publication catalog to prevent concurrent modifications during ownership change
- Searches publications by name using the PUBLICATIONNAME system cache
- Provides a clear error message when the specified publication does not exist
- Properly manages memory by freeing the heap tuple after processing
- Returns ObjectAddress for integration with PostgreSQL's object management system
- Serves as a wrapper around AlterPublicationOwner_internal, handling the publication lookup and resource management
- Used by the ALTER PUBLICATION OWNER TO command infrastructure

## Simplified Source

```c
ObjectAddress AlterPublicationOwner(const char *name, Oid newOwnerId)
{
    // Open publication catalog
    Relation rel = table_open(PublicationRelationId, RowExclusiveLock);

    // Find publication by name
    HeapTuple tup = SearchSysCacheCopy1(PUBLICATIONNAME, CStringGetDatum(name));

    if (!HeapTupleIsValid(tup))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("publication \"%s\" does not exist", name)));

    // Get publication OID
    Form_pg_publication pubform = (Form_pg_publication) GETSTRUCT(tup);
    Oid publication_oid = pubform->oid;

    // Delegate to internal function for actual ownership change
    AlterPublicationOwner_internal(rel, tup, newOwnerId);

    // Build return address
    ObjectAddress address;
    ObjectAddressSet(address, PublicationRelationId, publication_oid);

    // Cleanup
    heap_freetuple(tup);
    table_close(rel, RowExclusiveLock);

    return address;
}
```