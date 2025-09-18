# AlterPublicationOwner

## Location
src/backend/commands/publicationcmds.c: 1946 - 1980

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
  - table_open
  - SearchSysCacheCopy1
  - CStringGetDatum
  - AlterPublicationOwner_internal
  - ObjectAddressSet
  - heap_freetuple
  - table_close
  - Form_pg_publication (struct type)
- Called from (representative examples):
  - ExecAlterOwnerStmt

## Notes and Other Information
- This is a public function, accessible throughout the PostgreSQL codebase and declared in publicationcmds.h
- Uses RowExclusiveLock on the pg_publication catalog to prevent concurrent modifications during ownership change
- Searches publications by name using the PUBLICATIONNAME system cache
- Provides a clear error message when the specified publication does not exist
- Properly manages memory by freeing the heap tuple after processing
- Returns ObjectAddress for integration with PostgreSQL's object management system
- Serves as a wrapper around AlterPublicationOwner_internal, handling the publication lookup and resource management
- Used by the ALTER PUBLICATION OWNER TO command infrastructure